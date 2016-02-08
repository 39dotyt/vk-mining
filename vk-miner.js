#!/usr/bin/env node
/**
 * @license MIT
 * @author 0@39.yt (Yurij Mikhalevich)
 * @module 'vk-miner'
 */
'use strict';

const request = require('request');
const ActivityIndicator = require('./activity-indicator');

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
   * @param {string} method
   * @param {Object} parameters
   * @param {number} [retriesCount=0]
   * @returns {Promise}
   * @private
   */
  request_(method, parameters, retriesCount) {
    retriesCount = retriesCount || 0;
    this.activityIndicator.update();
    return new Promise((resolve, reject) => {
      const requestObject = Object.assign({lang: 'ru', v: '5.27'}, parameters);
      request.post(
          `https://api.vk.com/method/${method}`,
          {form: requestObject, timeout: 10000},
          (err, _, res) => {
            if (err) {
              reject(err);
              return;
            }
            try {
              res = JSON.parse(res);
            } catch (err) {
              reject(err);
              return;
            }
            if (!res.response) {
              let vkError;
              if (res.error) {
                vkError = new Error(res.error.error_msg);
                vkError.description = res.error;
              } else {
                vkError = new Error(JSON.stringify(res));
              }
              reject(vkError);
            } else {
              resolve(res.response);
            }
          }
      );
    }).then(null, err => {
      if (retriesCount > 3) throw err;
      console.warn(`vk request error: '${err.toString()}', retrying`);
      return sleep(++retriesCount * 10000);
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

    while (true) {
      try {
        yield posts.insertMany(items);
        break;
      } catch (err) {
        // 11000 - mongo duplicate key error
        // seems, like someone added a new post
        if (err.code === 11000) {
          items.shift();
        } else {
          throw err;
        }
      }
    }

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
    this.activityIndicator = new ActivityIndicator();
    yield this.loadWall_(pageId, posts, 0);
    this.activityIndicator.destroy();
    console.log(`- finished loading wall ${pageId} to db ${db.databaseName}, loaded ${yield posts.count()} posts`);
  }
}

module.exports = VKMiner;

if (module.parent) return;
const argv = require('yargs')
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
    const vkMiner = new VKMiner();
    yield vkMiner.loadWall(argv.pageId, db);
  } finally {
    yield db.close();
  }
}).catch(err => {
  console.error(err.stack);
});
