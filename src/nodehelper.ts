import { NodeProp, SortGroupsProp, SortSpaceMemory } from '@/enums';
import * as _ from 'lodash';
import { splitBalanced } from '@/services/help-service';
import Node from '@/inode';
import Worker from '@/iworker';
// Class to create nodes when parsing text
export default class NodeHelper {

  public parseWAL(text: string, el: Node): boolean {
    const WALRegex = /WAL:\s+(.*)\s*$/g;
    const WALMatches = WALRegex.exec(text);

    if (WALMatches) {
      // Initiate with default value
      _.each(['Records', 'Bytes', 'FPI'], (type) => {
        el['WAL ' + type] = 0;
      });
      _.each(WALMatches[1].split(/\s+/), (t) => {
        const s = t.split(/=/);
        const type = s[0];
        const value = parseInt(s[1], 0);
        let typeCaps;
        switch (type) {
          case 'fpi':
            typeCaps = 'FPI';
            break;
          default:
            typeCaps = _.capitalize(type);
        }
        el['WAL ' + typeCaps] = value;
      });
      return true;
    }

    return false;

  }

  public parseIOTimings(text: string, el: Node): boolean {

    /*
     * Groups
     */
    const iotimingsRegex = /I\/O Timings:\s+(.*)\s*$/g;
    const iotimingsMatches = iotimingsRegex.exec(text);

    /*
     * Groups:
     * 1: type
     * 2: info
     */
    if (iotimingsMatches) {
      // Initiate with default value
      el[NodeProp.IO_READ_TIME] = 0;
      el[NodeProp.IO_WRITE_TIME] = 0;

      _.each(iotimingsMatches[1].split(/\s+/), (timing) => {
        const s = timing.split(/=/);
        const method = s[0];
        const value = parseFloat(s[1]);
        const prop = 'IO_' + _.upperCase(method) + '_TIME' as keyof typeof NodeProp;
        el[NodeProp[prop]] = value;
      });
      return true;
    }
    return false;
  }

  public parseOptions(text: string, el: Node): boolean {
    // Parses an options block in JIT block
    // eg. Options: Inlining false, Optimization false, Expressions true, Deforming true

    /*
     * Groups
     */
    const optionsRegex = /^(\s*)Options:\s+(.*)$/g;
    const optionsMatches = optionsRegex.exec(text);

    if (optionsMatches) {
      el.Options = {};
      const options = optionsMatches[2].split(/\s*,\s*/);
      let matches;
      _.each(options, (option) => {
        const reg = /^(\S*)\s+(.*)$/g;
        matches = reg.exec(option);
        el.Options[matches![1]] = JSON.parse(matches![2]);
      });
      return true;
    }
    return false;
  }

  public parseTiming(text: string, el: Node): boolean {
    // Parses a timing block in JIT block
    // eg. Timing: Generation 0.340 ms, Inlining 0.000 ms, Optimization 0.168 ms, Emission 1.907 ms, Total 2.414 ms

    /*
     * Groups
     */
    const timingRegex = /^(\s*)Timing:\s+(.*)$/g;
    const timingMatches = timingRegex.exec(text);

    if (timingMatches) {
      el.Timing = {};
      const timings = timingMatches[2].split(/\s*,\s*/);
      let matches;
      _.each(timings, (option) => {
        const reg = /^(\S*)\s+(.*)$/g;
        matches = reg.exec(option);
        el.Timing[matches![1]] = this.parseTime(matches![2]);
      });
      return true;
    }
    return false;
  }

  public parseTime(text: string): number {
    return parseFloat(text.replace(/(\s*ms)$/, ''));
  }

  public parseSettings(text: string, el: Node): boolean {
    // Parses a settings block
    // eg. Timing: Generation 0.340 ms, Inlining 0.000 ms, Optimization 0.168 ms, Emission 1.907 ms, Total 2.414 ms

    const settingsRegex = /^(\s*)Settings:\s*(.*)$/g;
    const settingsMatches = settingsRegex.exec(text);

    if (settingsMatches) {
      el.Settings = {};
      const settings = splitBalanced(settingsMatches[2], ',');
      let matches;
      _.each(settings, (option) => {
        const reg = /^(\S*)\s+=\s+(.*)$/g;
        matches = reg.exec(_.trim(option));
        if (matches) {
          el.Settings[matches![1]] = matches![2].replace(/'/g, '');
        }
      });
      return true;
    }
    return false;
  }

  public parseSortGroups(text: string, el: Node): boolean {
    // Parses a Full-sort Groups block
    // eg. Full-sort Groups: 312500  Sort Method: quicksort  Average Memory: 26kB  Peak Memory: 26kB
    const sortGroupsRegex = /^\s*(Full-sort|Pre-sorted) Groups:\s+([0-9]*)\s+Sort Method[s]*:\s+(.*)\s+Average Memory:\s+(\S*)kB\s+Peak Memory:\s+(\S*)kB.*$/g;
    const matches = sortGroupsRegex.exec(text);

    if (matches) {
      const groups: {[key in SortGroupsProp]: any} = {
        [SortGroupsProp.GROUP_COUNT]: parseInt(matches[2], 0),
        [SortGroupsProp.SORT_METHODS_USED]: _.map(matches[3].split(','), _.trim),
        [SortGroupsProp.SORT_SPACE_MEMORY]: {
          [SortSpaceMemory.AVERAGE_SORT_SPACE_USED]: parseInt(matches[4], 0),
          [SortSpaceMemory.PEAK_SORT_SPACE_USED]: parseInt(matches[5], 0),
        },
      };

      if (matches[1] === 'Full-sort') {
        el[NodeProp.FULL_SORT_GROUPS] = groups;
      } else if (matches[1] === 'Pre-sorted') {
        el[NodeProp.PRE_SORTED_GROUPS] = groups;
      } else {
        throw new Error('Unsupported sort groups method');
      }
      return true;
    }
    return false;
  }

  public parseSortKey(text: string, el: Node): boolean {
    const sortRegex = /^\s*((?:Sort|Presorted) Key):\s+(.*)/g;
    const sortMatches = sortRegex.exec(text);
    if (sortMatches) {
      el[sortMatches[1]] = _.map(splitBalanced(sortMatches[2], ','), _.trim);
      return true;
    }
    return false;
  }

  public parseSort(text: string, el: Node | Worker): boolean {
    /*
     * Groups
     * 2: Sort Method
     * 3: Sort Space Type
     * 4: Sort Space Used
     */
    const sortRegex = /^(\s*)Sort Method:\s+(.*)\s+(Memory|Disk):\s+(?:(\S*)kB)\s*$/g;
    const sortMatches = sortRegex.exec(text);
    if (sortMatches) {
      el[NodeProp.SORT_METHOD] = sortMatches[2].trim();
      el[NodeProp.SORT_SPACE_USED] = sortMatches[4];
      el[NodeProp.SORT_SPACE_TYPE] = sortMatches[3];
      return true;
    }
    return false;
  }

  public parseBuffers(text: string, el: Node | Worker): boolean {
    /*
     * Groups
     */
    const buffersRegex = /Buffers:\s+(.*)\s*$/g;
    const buffersMatches = buffersRegex.exec(text);

    /*
     * Groups:
     * 1: type
     * 2: info
     */
    if (buffersMatches) {
      _.each(buffersMatches[1].split(/,\s+/), (infos) => {
        const bufferInfoRegex = /(shared|temp|local)\s+(.*)$/g;
        const m = bufferInfoRegex.exec(infos);
        if (m) {
          const type = m[1];
          // Initiate with default value
          _.each(['hit', 'read', 'written', 'dirtied'], (method) => {
            el[_.map([type, method, 'blocks'], _.capitalize).join(' ')] = 0;
          });
          _.each(m[2].split(/\s+/), (buffer) => {
            this.parseBuffer(buffer, type, el);
          });
        }
      });
      return true;
    }
    return false;
  }

  public parseBuffer(text: string, type: string, el: Node|Worker): void {
    const s = text.split(/=/);
    const method = s[0];
    const value = parseInt(s[1], 0);
    el[_.map([type, method, 'blocks'], _.capitalize).join(' ')] = value;
  }

  public calculateExclusives(node: Node) {
    // Caculate inclusive value for the current node for the given property
    const properties: Array<keyof typeof NodeProp> = [
      'SHARED_HIT_BLOCKS',
      'SHARED_READ_BLOCKS',
      'SHARED_DIRTIED_BLOCKS',
      'SHARED_WRITTEN_BLOCKS',
      'TEMP_READ_BLOCKS',
      'TEMP_WRITTEN_BLOCKS',
      'LOCAL_HIT_BLOCKS',
      'LOCAL_READ_BLOCKS',
      'LOCAL_DIRTIED_BLOCKS',
      'LOCAL_WRITTEN_BLOCKS',
      'IO_READ_TIME',
      'IO_WRITE_TIME',
    ];
    _.each(properties, (property) => {
      const sum = _.sumBy(
        node[NodeProp.PLANS],
        (child: Node) => {

          return child[NodeProp[property]] || 0;
        },
      );
      const exclusivePropertyString = 'EXCLUSIVE_' + property as keyof typeof NodeProp;
      node[NodeProp[exclusivePropertyString]] = node[NodeProp[property]] - sum;
    });
  }

  public findOutputProperty(node: Node): boolean {
    // resursively look for an "Output" property
    const children = node.Plans;
    if (!children) {
      return false;
    }
    return _.some(children, (child) => {
      return _.has(child, NodeProp.OUTPUT) || this.findOutputProperty(child);
    });
  }

}
