#!/usr/bin/env node
/**
 * @license MIT
 * @author 0@39.yt (Yurij Mikhalevich)
 * @module 'vk-miner'
 */
'use strict';

const VK = require('vksdk');

/**
 * @param {number} ms
 * @returns {Promise}
 */
function sleep(ms) {
  return new Promise(resolve => {
    setTimeout(() => resolve(), ms);
  });
}

class VKMiner {
  /**
   * @param {number} appId
   * @param {string} appSecret
   */
  constructor(appId, appSecret) {
    this.vk = new VK({appId, appSecret});
    this.vk.on('http-error', err => this.vkReject(err));
    this.vk.on('parse-error', err => this.vkReject(err));
  }

  /**
   * @param {string} method
   * @param {Object} parameters
   * @param {number} [retriesCount=0]
   * @returns {Promise}
   * @private
   */
  request_(method, parameters, retriesCount) {
    retriesCount = retriesCount || 0;
    return new Promise((resolve, reject) => {
      this.vkReject = reject;
      this.vk.request(method, parameters, res => {
        if (!res.response) {
          let err;
          if (res.error) {
            err = new Error(res.error.error_msg);
            err.description = res.error;
          } else {
            err = new Error(JSON.stringify(res));
          }
          reject(err);
        } else {
          resolve(res.response);
        }
      });
    }).then(res => {
      // vk API limits requests amount to 3 per second
      return sleep(20100).then(() => res);
    }, err => {
      if (retriesCount > 3) throw err;
      console.warn(`vk request error: '${err.toString()}', retrying`);
      return sleep(30000);
    }).then(res => {
      return res || this.request_(method, parameters, retriesCount);
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

    items.forEach(item => {
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

    console.log(`- loading wall ${pageId} to db ${db.databaseName}`);
    yield this.loadWall_(pageId, posts, 0);
    console.log(`- finished loading wall ${pageId} to db ${db.databaseName}, loaded ${yield posts.count()} posts`);
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

  try {
    const vkMiner = new VKMiner(argv.appId, argv.appSecret);
    yield vkMiner.loadWall(argv.pageId, db);
  } finally {
    yield db.close();
  }
}).catch(err => {
  console.error(err.stack);
});
