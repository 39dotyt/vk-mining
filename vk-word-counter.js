#!/usr/bin/env node
/**
 * @license MIT
 * @author 0@39.yt (Yurij Mikhalevich)
 * @module 'vk-word-counter'
 */
'use strict';

const MyStem = require('mystem3');
const meaninglessWords = require('./words-meaningless');

// next functions will be executed within MongoDB, so they should be in ES5

function map() {
  function extract(document) {
    var text = document.text;
    text = text.toLowerCase()
        .replace(/[:;,!?"'$\(\)\.]/g, ' ')
        .replace(/\[id\d{1,}\|[^\]]{1,}\]/g, ' ')
        .replace(/[\t\n ]{1,}/g, ' ').trim()
        .replace(/([\uE000-\uF8FF]|\uD83C[\uDF00-\uDFFF]|\uD83D[\uDC00-\uDE4F])/g, '');
    meaninglessWords.forEach(function(word) {
      var re = new RegExp('(^|\\s)' + word + '(^|\\s)', 'g');
      text = text.replace(re, '$1$2');
    });
    text = text.split(' ');
    text.forEach(function(word) {
      if (!isNaN(Number(word))) return;
      if (1 === word.length || word.length > 512) return;
      var result = {};
      var date = new Date(document.date * 1000);
      var monthIndex = `${date.getFullYear()}-${date.getMonth() + 1}`;
      result[monthIndex] = 1;
      emit(word, result);
    });
  }
  extract(this);
  this.comments.forEach(function(comment) {
    extract(comment);
  });
}

function reduce(key, values) {
  var result = {};
  values.forEach(function(value) {
    for (var monthIndex in value) {
      if (!value.hasOwnProperty(monthIndex)) continue;
      if (!result[monthIndex]) result[monthIndex] = 0;
      result[monthIndex] += value[monthIndex];
    }
  });
  return result;
}

function finalize(key, reducedValue) {
  var total = 0;
  for (var monthIndex in reducedValue) {
    if (!reducedValue.hasOwnProperty(monthIndex)) continue;
    total += reducedValue[monthIndex];
  }
  reducedValue.total = total;
  return reducedValue;
}

function phrasesMap() {
  function extract(text) {
    text = text.toLowerCase()
        .replace(/[:;,!?"'$\(\)\.]/g, ' ')
        .replace(/\[id\d{1,}\|[^\]]{1,}\]/g, '')
        .replace(/[\t\n ]{1,}/g, ' ').trim()
        .replace(/([\uE000-\uF8FF]|\uD83C[\uDF00-\uDFFF]|\uD83D[\uDC00-\uDE4F])/g, '');
    text = text.split(' ');
    var limit = text.length - phraseLength;
    for (var i = 0 ; i < limit; ++i) {
      var phrase = text.slice(i, i + phraseLength).join(' ');
      if (phrase.length > 512) continue;
      emit(phrase, 1);
    }
  }

  extract(this.text);
  this.comments.forEach(function (comment) {
    extract(comment.text);
  });
}

// end of mongodb functions

/**
 * @param {Object} collection
 * @param {Object} normalizedCollection
 */
function* normalize(collection, normalizedCollection) {
  const myStem = new MyStem();
  myStem.start();
  const cursor = collection.find();
  while (yield cursor.hasNext()) {
    const document = yield cursor.next();
    const normalized = yield myStem.lemmatize(document._id);
    const increment = {};
    for (let field in document.value) {
      if (!document.value.hasOwnProperty(field)) continue;
      increment[`value.${field}`] = document.value[field];
    }
    yield normalizedCollection.updateOne({_id: normalized}, {$inc: increment}, {upsert: true});
  }
  myStem.stop();
}

/**
 * @param {boolean} words
 * @param {Array<number>} phrases
 * @param {Object} db
 */
function* countWords(words, phrases, db) {
  const posts = db.collection('posts');

  if (words) {
    console.log(`- processing words in db ${db.databaseName}`);
    console.log(`\textracting words in db ${db.databaseName}`);
    // replace because function will be passed to Mongo as string
    const fMap = map.toString().replace('meaninglessWords', JSON.stringify(meaninglessWords));
    yield posts.mapReduce(fMap, reduce, {out: {replace: 'words'}, finalize});
    console.log(`\tfinished words extraction in db ${db.databaseName}`);
    console.log(`\tnormalizing words in db ${db.databaseName}`);
    const collections = yield db.listCollections().toArray();
    for (let i = 0; i < collections.length; ++i) {
      if (collections[i].name === 'wordsNorm') {
        yield db.dropCollection('wordsNorm');
        break;
      }
    }
    yield normalize(db.collection('words'), db.collection('wordsNorm'));
    console.log(`\tfinished words normalization in db ${db.databaseName}`);
    console.log(`- finished words processing in db ${db.databaseName}`);
  }

  for (let i = 0; i < phrases.length; ++i) {
    const phraseLength = phrases[i];
    if (phraseLength < 2) continue;
    const pfMap = phrasesMap.toString().replace(new RegExp('phraseLength', 'g'), phraseLength);
    console.log(`- extracting phrases with length of ${phraseLength}`);
    yield posts.mapReduce(pfMap, reduce, {out: {replace: `phrases-${phraseLength}`}, sort: {date: -1}, limit: 300});
    console.log(`- finished extraction of phrases with length of ${phraseLength}`);
    yield db.createIndex(`phrases-${phraseLength}`, {value: true});
  }
}

exports.countWords = countWords;

if (module.parent) return;
const argv = require('yargs')
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
const MongoClient = require('mongodb').MongoClient;

co(function*() {
  const db = yield MongoClient.connect(`mongodb://${argv.mongoHost}:${argv.mongoPort}/${argv.mongoDbName}`);
  try {
    yield countWords(argv.words, argv.phrases, db);
  } finally {
    yield db.close();
  }
}).catch(err => {
  console.error(err.stack);
});
