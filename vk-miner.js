#!/usr/bin/env node
/**
 * @license MIT
 * @author 0@39.yt (Yurij Mikhalevich)
 * @module 'vk-miner'
 */
'use strict';

const VK = require('vksdk');

class VKMiner {
  /**
   * @param {number} appId
   * @param {string} appSecret
   */
  constructor(appId, appSecret) {
    this.vk = new VK({
      appId: appId,
      appSecret: appSecret
    });
  }

  /**
   * @param {string} method
   * @param {Object} parameters
   * @returns {Promise}
   * @private
   */
  request_(method, parameters) {
    return new Promise((resolve, reject) => {
      this.vk.request(method, parameters, res => {
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

  /**
   * @param {number} pageId
   * @param {Object} posts Mongodb posts collection
   * @param {number} postId
   * @param {number} commentsOffset
   * @private
   */
  * loadComments_(pageId, posts, postId, commentsOffset) {
    const res = yield this.request_('wall.getComments', {
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
      yield this.loadComments_(pageId, posts, postId, commentsOffset);
    }
  }

  /**
   * @param {number} pageId
   * @param {Object} posts Mongodb posts collection
   * @param {number} postsOffset
   * @private
   */
  * loadWall_(pageId, posts, postsOffset) {
    const res = yield this.request_('wall.get', {
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
      yield this.loadComments_(pageId, posts, scheduledForComments[i], 0);
    }

    postsOffset += 100;
    if (postsOffset < res.count) {
      yield this.loadWall_(pageId, posts, postsOffset);
    }
  }

  /**
   * @param {number} pageId
   * @param {Object} db
   */
  * loadWall(pageId, db) {
    yield db.dropDatabase();
    const posts = db.collection('posts');
    yield db.createIndex('posts', {date: true});

    console.log('* loading wall');
    yield this.loadWall_(pageId, posts, 0);
    console.log(`* finished wall loading, loaded ${yield posts.count()} posts`);
  }
}

module.exports = VKMiner;

if (module.parent) return;
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
      demand: true,
      type: 'string'
    })
    .argv;
const co = require('co');
const MongoClient = require('mongodb').MongoClient;

co(function*() {
  const db = yield MongoClient.connect(`mongodb://${argv.mongoHost}:${argv.mongoPort}/${argv.mongoDbName}`);

  const vkMiner = new VKMiner(argv.appId, argv.appSecret);
  yield vkMiner.loadWall(argv.pageId, db);

  db.close();
}).catch(err => {
  console.error(err.stack);
});
