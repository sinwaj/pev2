import * as _ from 'lodash';
import {BufferLocation, EstimateDirection, SortGroupsProp, NodeProp, SortSpaceMemory, WorkerProp} from '@/enums';
import {splitBalanced} from '@/services/help-service';
import { IPlan } from '@/iplan';
import Node from '@/inode';
import Worker from '@/iworker';
import moment from 'moment';
import clarinet from 'clarinet';

export class SourceParser {

  public cleanupSource(source: string) {
    // Remove frames around, handles |, ║,
    source = source.replace(/^(\||║|│)(.*)\1\r?\n/gm, '$2\n');

    // Remove separator lines from various types of borders
    source = source.replace(/^\+-+\+\r?\n/gm, '');
    source = source.replace(/^(-|─|═)\1+\r?\n/gm, '');
    source = source.replace(/^(├|╟|╠|╞)(─|═)\2*(┤|╢|╣|╡)\r?\n/gm, '');

    // Remove more horizontal lines
    source = source.replace(/^\+-+\+\r?\n/gm, '');
    source = source.replace(/^└(─)+┘\r?\n/gm, '');
    source = source.replace(/^╚(═)+╝\r?\n/gm, '');
    source = source.replace(/^┌(─)+┐\r?\n/gm, '');
    source = source.replace(/^╔(═)+╗\r?\n/gm, '');

    // Remove quotes around lines, both ' and "
    source = source.replace(/^(["'])(.*)\1\r?\n/gm, '$2\n');

    // Remove "+" line continuations
    source = source.replace(/\s*\+\r?\n/g, '\n');

    // Remove "↵" line continuations
    source = source.replace(/↵\r?/gm, '\n');

    // Remove "query plan" header
    source = source.replace(/^\s*QUERY PLAN\s*\r?\n/m, '');

    // Remove rowcount
    // example: (8 rows)
    // Note: can be translated
    // example: (8 lignes)
    source = source.replace(/^\(\d+\s+[a-z]*s?\)(\r?\n|$)/gm, '\n');

    return source;
  }



  public fromJson(source: string) {
    // We need to remove things before and/or after explain
    // To do this, first - split explain into lines...
    const sourceLines = source.split(/[\r\n]+/);

    // Now, find first line of explain, and cache it's prefix (some spaces ...)
    let prefix = '';
    let firstLineIndex = 0;
    _.each(sourceLines, (l: string, index: number) => {
      const matches = /^(\s*)(\[|\{)\s*$/.exec(l);
      if (matches) {
        prefix = matches[1];
        firstLineIndex = index;
        return false;
      }
    });
    // now find last line
    let lastLineIndex = 0;
    _.each(sourceLines, (l: string, index: number) => {
      const matches = new RegExp('^' + prefix + '(\]|\})\s*$').exec(l);
      if (matches) {
        lastLineIndex = index;
        return false;
      }
    });

    const useSource: string = sourceLines.slice(firstLineIndex, lastLineIndex + 1).join('\n');

    return this.parseJson(useSource);
  }

  // Stream parse JSON as it can contain duplicate keys (workers)
  public parseJson(source: string) {
    const parser = clarinet.parser();
    const elements: any[] = [];
    let root: any = null;
    // Store the level and duplicated object|array
    let duplicated: [number, any] | null = null;
    parser.onvalue = (v: any) => {
      const current = elements[elements.length - 1];
      if (_.isArray(current)) {
        current.push(v);
      } else {
        const keys = Object.keys(current);
        const lastKey = keys[keys.length - 1];
        current[lastKey] = v;
      }
    };
    parser.onopenobject = (key: any) => {
      const o: {[key: string]: any} = {};
      o[key] = null;
      elements.push(o);
    };
    parser.onkey = (key: any) => {
      const current = elements[elements.length - 1];
      const keys = Object.keys(current);
      if (keys.indexOf(key) !== -1) {
        duplicated = [elements.length - 1, current[key]];
      } else {
        current[key] = null;
      }
    };
    parser.onopenarray = () => {
      elements.push([]);
    };
    parser.oncloseobject = parser.onclosearray = () => {
      const popped = elements.pop();

      if (!elements.length) {
        root = popped;
      } else {
        const current = elements[elements.length - 1];

        if (duplicated && duplicated[0] === elements.length - 1) {
          _.merge(duplicated[1], popped);
          duplicated = null;
        } else {
          if (_.isArray(current)) {
            current.push(popped);
          } else {
            const keys = Object.keys(current);
            const lastKey = keys[keys.length - 1];
            current[lastKey] = popped;
          }
        }
      }
    };
    parser.write(source).close();
    if (root instanceof Array) {
      root = root[0];
    }
    return root;
  }

  public splitIntoLines(text: string): string[] {
    // Splits source into lines, while fixing (well, trying to fix)
    // cases where input has been force-wrapped to some length.
    const out: string[] = [];
    const lines = text.split(/\r?\n/);
    const countChar = (str: string, ch: RegExp) => (str.match(ch) || []).length;

    _.each(lines, (line: string) => {
      if (countChar(line, /\)/g) > countChar(line, /\(/g)) {
        // if there more closing parenthesis this means that it's the
        // continuation of a previous line
        out[out.length - 1] += line;
      } else if (line.match(/^(?:Total\s+runtime|Planning\s+time|Execution\s+time|Time|Filter|Output|JIT)/i)) {
        out.push(line);
      } else if (
        line.match(/^\S/) || // doesn't start with a blank space (allowed only for the first node)
        line.match(/^\s*\(/) // first non-blank character is an opening parenthesis
      ) {
        if (0 < out.length) {
          out[out.length - 1] += line;
        } else {
          out.push(line);
        }
      } else {
        out.push(line);
      }
    });
    return out;
  }
}
