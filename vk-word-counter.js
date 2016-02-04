/**
 * @license MIT
 * @author 0@39.yt (Yurij Mikhalevich)
 * @module 'vk-word-counter'
 */
'use strict';

var MongoClient = require('mongodb').MongoClient;
var MyStem = require('mystem3');
var myStem = new MyStem();
myStem.start();

var meaninglessWords = require('./words-meaningless');
var settings = require('./settings');

var dbURI = settings.dbURI;

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
      var monthIndex = (date.getFullYear() * 12) + date.getMonth() + 1;
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


MongoClient.connect(dbURI, function(err, db) {
  if (err) throw err;

  var posts = db.collection('posts');

  var fMap = map.toString().replace('meaninglessWords', JSON.stringify(meaninglessWords));

  posts.mapReduce(fMap, reduce, {out: {replace: 'words'}, finalize: finalize}, function() {
    console.log('finished words', err);
    console.log('starting normalization process');
    normalize(db.collection('words'), db.collection('wordsNorm'));
  });

  //for (var length = 2; length <= 5; ++length) {
  //  var pfMap = phrasesMap.toString().replace(new RegExp('phraseLength', 'g'), length.toString());
  //  posts.mapReduce(pfMap, reduce, {out: 'phrases-' + length, sort: {date: -1}, limit: 300}, function() {
  //    console.log('finished phrases-' + this.length, err);
  //    db.createIndex('phrases-' + this.length.toString(), {value: true});
  //  }.bind({length: length}));
  //}
});

var queue = [];

function normalize(collection, normalizedCollection) {
  var stream = collection.find().stream();
  stream.on('end', function() {
    console.log('normalization prepared');
    startQueue();
  });
  stream.on('data', function(document) {
    enqueue(function(cb) {
      // overall enqueue mechanism is very unoptimal, shitty piece of code
      // i will not fix it right now in the fight against perfectionism
      myStem.lemmatize(document._id).then(function(normalized) {
        var increment = {};
        for (var field in document.value) {
          if (!document.value.hasOwnProperty(field)) continue;
          increment['value.' + field.toString()] = document.value[field];
        }
        normalizedCollection.updateOne({_id: normalized}, {$inc: increment}, {upsert: true}, function (err) {
          if (err) console.error('normalization err', err);
          cb();
        });
      }).catch(console.error);
    });
  });
}

function done() {
  if (!queue.length) {
    console.log('normalization finished');
    return;
  }
  var fn = queue.shift();
  fn(done);
}

function enqueue(fn) {
  queue.push(fn);
}

function startQueue() {
  console.log('normalization started');
  done();
}
