#!/usr/bin/env node
/**
 * @license MIT
 * @author 0@39.yt (Yurij Mikhalevich)
 * @module 'vk-miner'
 */
'use strict';

const argv = require('yargs')
    .option('appId', {
      alias: 'a',
      demand: true,
      type: 'number'
    })
    .option('appSecret', {
      alias: 's',
      demand: true
    })
    .option('pageId', {
      alias: 'i',
      demand: true,
      type: 'number',
      nargs: 1
    })
    .option('mongoPort', {
      alias: 'P',
      default: 27017,
      type: 'number'
    })
    .option('mongoHost', {
      alias: 'H',
      default: 'localhost'
    })
    .option('mongoDbName', {
      alias: 'D',
      default: 'vk-miner'
    })
    .argv;
const co = require('co');
const VK = require('vksdk');
const MongoClient = require('mongodb').MongoClient;
const settings = require('./settings');

const vk = new VK({
  appId: argv.appId,
  appSecret: argv.appSecret
});

const pageId = argv.pageId;

let db;
let posts;

function vkRequest(method, parameters) {
  return new Promise((resolve, reject) => {
    vk.request(method, parameters, res => {
      if (res.error) {
        const err = new Error(res.error.error_msg);
        err.description = res.error;
        reject(err);
      } else {
        resolve(res.response);
      }
    });
  });
}

function* commentsLoad(postId, commentsOffset) {
  commentsOffset = commentsOffset || 0;
  const res = yield vkRequest('wall.getComments', {
    owner_id: pageId,
    post_id: postId,
    count: 100,
    offset: commentsOffset,
    need_likes: 1,
    preview_length: 0
  });

  const update = yield posts.updateOne({_id: postId}, {$push: {comments: {$each: res.items}}});
  if (!update.result.ok || !update.result.nModified) throw new Error(update.result);

  commentsOffset += 100;
  if (commentsOffset < res.count) {
    yield commentsLoad(postId, commentsOffset);
  }
}

function* wallLoad(postsOffset) {
  const res = yield vkRequest('wall.get', {
    owner_id: pageId,
    count: 100,
    offset: postsOffset
  });
  const items = res.items;

  const scheduledForComments = [];

  items.forEach(function(item) {
    if (item.comments && item.comments.count) scheduledForComments.push(item.id);
    item.comments = [];
    item._id = item.id;
    delete item.id;
  });

  yield posts.insertMany(items);

  for (let i = 0; i < scheduledForComments.length; ++i) {
    yield commentsLoad(scheduledForComments[i]);
  }

  postsOffset += 100;
  if (postsOffset < res.count) {
    yield wallLoad(postsOffset);
  }
}

co(function*() {
  db = yield MongoClient.connect(`mongodb://${argv.mongoHost}:${argv.mongoPort}/${argv.mongoDbName}`);
  db.dropDatabase();

  posts = db.collection('posts');

  db.createIndex('posts', {date: true});

  yield wallLoad(0);
  console.log('wall was loaded');
  db.close();
}).catch(err => {
  console.error(err.stack);
});
