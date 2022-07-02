import { NodeProp, SortGroupsProp, SortSpaceMemory } from '@/enums';
import * as _ from 'lodash';
import { splitBalanced } from '@/services/help-service';
// Class to create nodes when parsing text
export default class Node {
  [k: string]: any;
  constructor(type: string) {
    this[NodeProp.NODE_TYPE] = type;
    // tslint:disable-next-line:max-line-length
    const scanMatches = /^((?:Parallel\s+)?(?:Seq\sScan|Tid.*Scan|Bitmap\s+Heap\s+Scan|(?:Async\s+)?Foreign\s+Scan|Update|Insert|Delete))\son\s(\S+)(?:\s+(\S+))?$/.exec(type);
    const bitmapMatches = /^(Bitmap\s+Index\s+Scan)\son\s(\S+)$/.exec(type);
    // tslint:disable-next-line:max-line-length
    const indexMatches = /^((?:Parallel\s+)?Index(?:\sOnly)?\sScan(?:\sBackward)?)\susing\s(\S+)\son\s(\S+)(?:\s+(\S+))?$/.exec(type);
    const cteMatches = /^(CTE\sScan)\son\s(\S+)(?:\s+(\S+))?$/.exec(type);
    const functionMatches = /^(Function\sScan)\son\s(\S+)(?:\s+(\S+))?$/.exec(type);
    const subqueryMatches = /^(Subquery\sScan)\son\s(.+)$/.exec(type);
    if (scanMatches) {
      this[NodeProp.NODE_TYPE] = scanMatches[1];
      this[NodeProp.RELATION_NAME] = scanMatches[2];
      if (scanMatches[3]) {
        this[NodeProp.ALIAS] = scanMatches[3];
      }
    } else if (bitmapMatches) {
      this[NodeProp.NODE_TYPE] = bitmapMatches[1];
      this[NodeProp.INDEX_NAME] = bitmapMatches[2];
    } else if (indexMatches) {
      this[NodeProp.NODE_TYPE] = indexMatches[1];
      this[NodeProp.INDEX_NAME] = indexMatches[2];
      this[NodeProp.RELATION_NAME] = indexMatches[3];
      if (indexMatches[4]) {
        this[NodeProp.ALIAS] = indexMatches[4];
      }
    } else if (cteMatches) {
      this[NodeProp.NODE_TYPE] = cteMatches[1];
      this[NodeProp.CTE_NAME] = cteMatches[2];
      if (cteMatches[3]) {
        this[NodeProp.ALIAS] = cteMatches[3];
      }
    } else if (functionMatches) {
      this[NodeProp.NODE_TYPE] = functionMatches[1];
      this[NodeProp.FUNCTION_NAME] = functionMatches[2];
      if (functionMatches[3]) {
        this[NodeProp.ALIAS] = functionMatches[3];
      }
    } else if (subqueryMatches) {
      this[NodeProp.NODE_TYPE] = subqueryMatches[1];
      // this[NodeProp.SUBQUERY_NAME] = subqueryMatches[2].replace
    }
    const parallelMatches = /^(Parallel\s+)(.*)/.exec(this[NodeProp.NODE_TYPE]);
    if (parallelMatches) {
      this[NodeProp.NODE_TYPE] = parallelMatches[2];
      this[NodeProp.PARALLEL_AWARE] = true;
    }

    const joinMatches = /(.*)\sJoin$/.exec(this[NodeProp.NODE_TYPE]);
    const joinModifierMatches = /(.*)\s+(Full|Left|Right|Anti)/.exec(this[NodeProp.NODE_TYPE]);
    if (joinMatches) {
      this[NodeProp.NODE_TYPE] = joinMatches[1];
      if (joinModifierMatches) {
        this[NodeProp.NODE_TYPE] = joinModifierMatches[1];
        this[NodeProp.JOIN_TYPE] = joinModifierMatches[2];
      }
      this[NodeProp.NODE_TYPE] += ' Join';
    }

  }

  public hello() {
    const i = 0;
  }

  public parseWAL(text: string): boolean {
    const el = this;
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

  public parseTiming(text: string): boolean {
    const el = this;
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

  public parseSettings(text: string): boolean {
    const el = this;
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

  public parseSortGroups(text: string): boolean {
    const el = this;
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

  public calculateExclusives() {
    const node = this;
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



  public parseIOTimings(text: string): boolean {
   const el = this;
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

  public parseOptions(text: string): boolean {
    const el = this;
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
  public parseSortKey(text: string): boolean {
    const el = this;
    const sortRegex = /^\s*((?:Sort|Presorted) Key):\s+(.*)/g;
    const sortMatches = sortRegex.exec(text);
    if (sortMatches) {
      el[sortMatches[1]] = _.map(splitBalanced(sortMatches[2], ','), _.trim);
      return true;
    }
    return false;
  }

}
