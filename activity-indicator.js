/**
 * @license MIT
 * @author 0@39.yt (Yurij Mikhalevich)
 * @module 'activity-indicator'
 */
'use strict';
const readline = require('readline');

const states = ['/', '|', '\\', '-'];
const statesAmount = states.length;

class ActivityIndicator {
  constructor() {
    this.state = 0;
    process.stdout.write(states[this.state]);
  }
  update() {
    readline.clearLine(process.stdout, -1);
    readline.cursorTo(process.stdout, 0);
    process.stdout.write(states[++this.state % statesAmount]);
  }
  destroy() {
    readline.clearLine(process.stdout, -1);
    readline.cursorTo(process.stdout, 0);
  }
}

module.exports = ActivityIndicator;
