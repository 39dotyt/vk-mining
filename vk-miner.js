/**
 * @license GPL-3.0+
 * @author 0@39.yt (Yurij Mikhalevich)
 */
var VK = require('vksdk');
var MongoClient = require('mongodb').MongoClient;
var settings = require('./settings');

var vk = new VK({
  appId: settings.appId,
  appSecret: settings.appSecret
});

var publicId = settings.publicId;
var dbURI = settings.dbURI;

var db;
var posts;

function commentsLoad(postId, commentsOffset) {
  commentsOffset = commentsOffset || 0;
  vk.request('wall.getComments', {
    owner_id: publicId,
    post_id: postId,
    count: 100,
    offset: commentsOffset,
    need_likes: 1,
    preview_length: 0
  }, function(res) {
    if (!res.response) {
      console.error(res);
      return;
    }

    var comments = res.response.items;
    posts.updateOne({_id: postId}, {$push: {comments: {$each: comments}}}, function(err, updated) {
      if (err || !updated.result.ok || !updated.result.nModified) console.error(err || updated.result, postId);
    });


    commentsOffset += 100;
    if (commentsOffset < res.response.count) {
      commentsLoad(postId, commentsOffset);
    }
  });
}

function wallLoad(postsOffset) {
  vk.request('wall.get', {
    owner_id: publicId,
    count: 100,
    offset: postsOffset
  }, function(res) {
    if (!res.response) {
      console.error(res);
      return;
    }

    var items = res.response.items;

    var sheduledForComments = [];

    items.forEach(function(item) {
      if (item.comments && item.comments.count) sheduledForComments.push(item.id);
      item.comments = [];
      item._id = item.id;
      delete item.id;
    });

    posts.insertMany(items, function(err) {
      if (err) {
        console.error(err);
        return;
      }
      sheduledForComments.forEach(function(postId) {
        commentsLoad(postId);
      });
    });

    postsOffset += 100;
    if (postsOffset < res.response.count) {
      wallLoad(postsOffset);
    } else {
      console.log("finished loading wall");
    }
  });
}

MongoClient.connect(dbURI, function(err, connectedDb) {
  if (err) throw err;

  db = connectedDb;
  posts = db.collection('posts');

  db.createIndex('posts', {date: true});

  wallLoad(0);
});
