#!/usr/bin/env node
/**
 * @license MIT
 * @author 0@39.yt (Yurij Mikhalevich)
 * @module 'mine-and-count'
 */
'use strict';

const VKMiner = require('./vk-miner');
const wordCounter = require('./vk-word-counter');
const MongoClient = require('mongodb').MongoClient;

/**
 * @typedef {Object} Page
 * @property {number} id
 * @property {string} dbName
 */

/**
 * @param {number} appId
 * @param {string} appSecret
 * @param {Array<Page>} pages
 * @param {string} mongoHost
 * @param {number} mongoPort
 * @param {boolean} words
 * @param {Array<number>} phrases
 */
function* mineAndCount(appId, appSecret, pages, mongoHost, mongoPort, words, phrases) {
  const vkMiner = new VKMiner(appId, appSecret);

  for (let i = 0; i < pages.length; ++i) {
    const page = pages[i];
    const db = yield MongoClient.connect(`mongodb://${mongoHost}:${mongoPort}/${page.dbName}`);
    try {
      yield vkMiner.loadWall(page.id, db);
      yield wordCounter.countWords(words, phrases, db);
    } finally {
      yield db.close();
    }
  }
}

module.exports = mineAndCount;

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
    .option('words', {
      alias: 'w',
      default: true,
      type: 'boolean'
    })
    .option('phrases', {
      alias: 'p',
      default: [],
      type: 'array',
      description: 'enables experimental phrases extraction',
      usage: 'specify the list of phrases length, for example: 2,3,4,' +
      'note that work with long phrases may took significant time'
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

co(function*() {
  yield mineAndCount(
      argv.appId, argv.appSecret,
      [{id: argv.pageId, dbName: argv.mongoDbName}],
      argv.mongoHost, argv.mongoPort,
      argv.words, argv.phrases
  );
}).catch(err => {
  console.error(err.stack);
});
