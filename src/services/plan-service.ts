import * as _ from 'lodash';
import {BufferLocation, EstimateDirection, SortGroupsProp, NodeProp, SortSpaceMemory, WorkerProp} from '@/enums';
import {splitBalanced} from '@/services/help-service';
import { IPlan } from '@/iplan';
import Node from '@/inode';
import Worker from '@/iworker';
import moment from 'moment';
import clarinet from 'clarinet';
import NodeHelper from '@/nodehelper';
import { SourceParser } from '@/services/sourceparser';



export class PlanService {

  private static instance: PlanService;
  private nodeId: number = 0;
  private nodeHelper: NodeHelper = new NodeHelper();
  private sourcePareser: SourceParser = new SourceParser();

  public createPlan(planName: string, planContent: any, planQuery: string): IPlan {


    // remove any extra white spaces in the middle of query
    // (\S) start match after any non-whitespace character => group 1
    // (?!$) don't start match after end of line
    // (\s{2,}) group of 2 or more white spaces
    // '$1 ' reuse group 1 and and a single space
    planQuery = planQuery.replace(/(\S)(?!$)(\s{2,})/gm, '$1 ');

    const plan: IPlan = {
      id: NodeProp.PEV_PLAN_TAG + new Date().getTime().toString(),
      name: planName || 'plan created on ' + moment().format('LLL'),
      createdOn: new Date(),
      content: planContent,
      query: planQuery,
      planStats: {},
      ctes: [],
      isAnalyze: _.has(planContent.Plan, NodeProp.ACTUAL_ROWS),
      isVerbose: this.nodeHelper.findOutputProperty(planContent.Plan),
    };

    this.nodeId = 1;
    this.processNode(plan.content.Plan, plan);
    this.calculateMaximums(plan.content);
    return plan;
  }

  public isCTE(node: any) {
    return node[NodeProp.PARENT_RELATIONSHIP] === 'InitPlan' &&
      _.startsWith(node[NodeProp.SUBPLAN_NAME], 'CTE');
  }

  // recursively walk down the plan to compute various metrics
  public processNode(node: any, plan: any) {
    node.nodeId = this.nodeId++;
    this.calculatePlannerEstimate(node);

    _.each(node[NodeProp.PLANS], (child) => {
      // Disseminate workers planned info to parallel nodes (ie. Gather children)
      if (!this.isCTE(child) &&
          child[NodeProp.PARENT_RELATIONSHIP] !== 'InitPlan' &&
          child[NodeProp.PARENT_RELATIONSHIP] !== 'SubPlan') {
        child[NodeProp.WORKERS_PLANNED_BY_GATHER] = node[NodeProp.WORKERS_PLANNED] ||
          node[NodeProp.WORKERS_PLANNED_BY_GATHER];
      }
      if (this.isCTE(child)) {
        plan.ctes.push(child);
      }
      this.processNode(child, plan);
    });

    _.remove(node[NodeProp.PLANS], (child: any) => this.isCTE(child));

    // calculate actuals after processing child nodes so that actual duration
    // takes loops into account
    this.calculateActuals(node);
    this.nodeHelper.calculateExclusives(node);

  }

  public calculateMaximums(content: any) {
    function recurse(nodes: any[]): any[] {
      return _.map(nodes, (node) => [node, recurse(node[NodeProp.PLANS])]);
    }
    const flat = _.flattenDeep(recurse([content.Plan as IPlan]));

    const largest = _.maxBy(flat, NodeProp.ACTUAL_ROWS);
    if (largest) {
      content.maxRows = largest[NodeProp.ACTUAL_ROWS];
    }

    const costliest = _.maxBy(flat, NodeProp.EXCLUSIVE_COST);
    if (costliest) {
      content.maxCost = costliest[NodeProp.EXCLUSIVE_COST];
    }

    const totalCostliest = _.maxBy(flat, NodeProp.TOTAL_COST);
    if (totalCostliest) {
      content.maxTotalCost = totalCostliest[NodeProp.TOTAL_COST];
    }

    const slowest = _.maxBy(flat, NodeProp.EXCLUSIVE_DURATION);
    if (slowest) {
      content.maxDuration = slowest[NodeProp.EXCLUSIVE_DURATION];
    }

    if (!content.maxBlocks) {
      content.maxBlocks = {};
    }

    function sumShared(o: Node) {
      return o[NodeProp.EXCLUSIVE_SHARED_HIT_BLOCKS] +
        o[NodeProp.EXCLUSIVE_SHARED_READ_BLOCKS] +
        o[NodeProp.EXCLUSIVE_SHARED_DIRTIED_BLOCKS] +
        o[NodeProp.EXCLUSIVE_SHARED_WRITTEN_BLOCKS];
    }
    const highestShared = _.maxBy(flat, (o) => {
      return sumShared(o);
    });
    if (highestShared && sumShared(highestShared)) {
      content.maxBlocks[BufferLocation.shared] = sumShared(highestShared);
    }

    function sumTemp(o: Node) {
      return o[NodeProp.EXCLUSIVE_TEMP_READ_BLOCKS] +
        o[NodeProp.EXCLUSIVE_TEMP_WRITTEN_BLOCKS];
    }
    const highestTemp = _.maxBy(flat, (o) => {
      return sumTemp(o);
    });
    if (highestTemp && sumTemp(highestTemp)) {
      content.maxBlocks[BufferLocation.temp] = sumTemp(highestTemp);
    }

    function sumLocal(o: Node) {
      return o[NodeProp.EXCLUSIVE_LOCAL_HIT_BLOCKS] +
        o[NodeProp.EXCLUSIVE_LOCAL_READ_BLOCKS] +
        o[NodeProp.EXCLUSIVE_LOCAL_DIRTIED_BLOCKS] +
        o[NodeProp.EXCLUSIVE_LOCAL_WRITTEN_BLOCKS];
    }
    const highestLocal = _.maxBy(flat, (o) => {
      return sumLocal(o);
    });
    if (highestLocal && sumLocal(highestLocal)) {
      content.maxBlocks[BufferLocation.local] = sumLocal(highestLocal);
    }
  }

  // actual duration and actual cost are calculated by subtracting child values from the total
  public calculateActuals(node: any) {
    if (!_.isUndefined(node[NodeProp.ACTUAL_TOTAL_TIME])) {
      // since time is reported for an invidual loop, actual duration must be adjusted by number of loops
      // number of workers is also taken into account
      const workers = (node[NodeProp.WORKERS_PLANNED_BY_GATHER] || 0) + 1;
      node[NodeProp.ACTUAL_TOTAL_TIME] = node[NodeProp.ACTUAL_TOTAL_TIME] * node[NodeProp.ACTUAL_LOOPS] / workers;
      node[NodeProp.ACTUAL_STARTUP_TIME] = node[NodeProp.ACTUAL_STARTUP_TIME] * node[NodeProp.ACTUAL_LOOPS] / workers;
      node[NodeProp.EXCLUSIVE_DURATION] = node[NodeProp.ACTUAL_TOTAL_TIME];

      const duration = node[NodeProp.EXCLUSIVE_DURATION] - this.childrenDuration(node, 0);
      node[NodeProp.EXCLUSIVE_DURATION] = duration > 0 ? duration : 0;
    }

    if (node[NodeProp.TOTAL_COST]) {
      node[NodeProp.EXCLUSIVE_COST] = node[NodeProp.TOTAL_COST];
    }


    _.each(node[NodeProp.PLANS], (subPlan) => {
      if (subPlan[NodeProp.PARENT_RELATIONSHIP] !== 'InitPlan' && subPlan[NodeProp.TOTAL_COST]) {
        node[NodeProp.EXCLUSIVE_COST] = node[NodeProp.EXCLUSIVE_COST] - subPlan[NodeProp.TOTAL_COST];
      }
    });

    if (node[NodeProp.EXCLUSIVE_COST] < 0) {
      node[NodeProp.EXCLUSIVE_COST] = 0;
    }

    _.each(['ACTUAL_ROWS', 'PLAN_ROWS', 'ROWS_REMOVED_BY_FILTER', 'ROWS_REMOVED_BY_JOIN_FILTER'],
      (prop: keyof typeof NodeProp) => {
      if (!_.isUndefined(node[NodeProp[prop]])) {
        const revisedProp = prop + '_REVISED' as keyof typeof NodeProp;
        const loops = node[NodeProp.ACTUAL_LOOPS] || 1;
        node[NodeProp[revisedProp]] = node[NodeProp[prop]] * loops;
      }
    });
  }

  // recursive function to get the sum of actual durations of a a node children
  public childrenDuration(node: Node, duration: number) {
    _.each(node[NodeProp.PLANS], (child) => {
      // Subtract sub plans duration from this node except for InitPlans
      // (ie. CTE)
      if (child[NodeProp.PARENT_RELATIONSHIP] !== 'InitPlan') {
        duration += child[NodeProp.EXCLUSIVE_DURATION] || 0; // Duration may not be set
        duration = this.childrenDuration(child, duration);
      }
    });
    return duration;
  }

  // figure out order of magnitude by which the planner mis-estimated how many rows would be
  // invloved in this node
  public calculatePlannerEstimate(node: any) {
    if (node[NodeProp.ACTUAL_ROWS] === undefined) {
      return;
    }
    node[NodeProp.PLANNER_ESTIMATE_FACTOR] = node[NodeProp.ACTUAL_ROWS] / node[NodeProp.PLAN_ROWS];
    node[NodeProp.PLANNER_ESTIMATE_DIRECTION] = EstimateDirection.none;

    if (node[NodeProp.ACTUAL_ROWS] > node[NodeProp.PLAN_ROWS]) {
      node[NodeProp.PLANNER_ESTIMATE_DIRECTION] = EstimateDirection.under;
    }
    if (node[NodeProp.ACTUAL_ROWS] < node[NodeProp.PLAN_ROWS]) {
      node[NodeProp.PLANNER_ESTIMATE_DIRECTION] = EstimateDirection.over;
      node[NodeProp.PLANNER_ESTIMATE_FACTOR] = node[NodeProp.PLAN_ROWS] / node[NodeProp.ACTUAL_ROWS];
    }
  }



  public fromSource(source: string) {
    source = this.sourcePareser.cleanupSource(source);

    let isJson = false;
    try {
      isJson =  JSON.parse(source);
    } catch (error) {
      // continue
    }

    if (isJson) {
      return this.sourcePareser.parseJson(source);
    } else if (/^(\s*)(\[|\{)\s*\n.*?\1(\]|\})\s*/gms.exec(source)) {
      return this.sourcePareser.fromJson(source);
    }
    return this.fromText(source);
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

  public fromText(text: string) {
    const lines = this.splitIntoLines(text);

    const root: any = {};
    root.Plan = null;
    type ElementAtDepth = [number, any];
    // Array to keep reference to previous nodes with there depth
    const elementsAtDepth: ElementAtDepth[] = [];

    _.each(lines, (line: string) => {
      // Remove any trailing "
      line = line.replace(/"\s*$/, '');
      // Remove any begining "
      line = line.replace(/^\s*"/, '');
      // Replace tabs with 4 spaces
      line = line.replace(/\t/gm, '    ');

      const indentationRegex = /^\s*/;
      const depth = line.match(indentationRegex)![0].length;
      // remove indentation
      line = line.replace(indentationRegex, '');

      const emptyLineRegex = '^\s*$';
      const headerRegex = '^\\s*(QUERY|---|#).*$';
      const prefixRegex = '^(\\s*->\\s*|\\s*)';
      // const typeRegex = '([^\\r\\n\\t\\f\\v\\:\\(]*?)';
      const typeRegex = '([^\\r\\n\\t\\f\\v]*?)';
      // tslint:disable-next-line:max-line-length
      const estimationRegex = '\\(cost=(\\d+\\.\\d+)\\.\\.(\\d+\\.\\d+)\\s+rows=(\\d+)\\s+width=(\\d+)\\)';
      const nonCapturingGroupOpen = '(?:';
      const nonCapturingGroupClose = ')';
      const openParenthesisRegex = '\\(';
      const closeParenthesisRegex = '\\)';
      // tslint:disable-next-line:max-line-length
      const actualRegex = '(?:actual\\stime=(\\d+\\.\\d+)\\.\\.(\\d+\\.\\d+)\\srows=(\\d+)\\sloops=(\\d+)|actual\\srows=(\\d+)\\sloops=(\\d+)|(never\\s+executed))';
      const optionalGroup = '?';

      const emptyLineMatches = new RegExp(emptyLineRegex).exec(line);
      const headerMatches = new RegExp(headerRegex).exec(line);

      /*
       * Groups
       * 1: prefix
       * 2: type
       * 3: estimated_startup_cost
       * 4: estimated_total_cost
       * 5: estimated_rows
       * 6: estimated_row_width
       * 7: actual_time_first
       * 8: actual_time_last
       * 9: actual_rows
       * 10: actual_loops
       * 11: actual_rows_
       * 12: actual_loops_
       * 13: never_executed
       * 14: estimated_startup_cost
       * 15: estimated_total_cost
       * 16: estimated_rows
       * 17: estimated_row_width
       * 18: actual_time_first
       * 19: actual_time_last
       * 20: actual_rows
       * 21: actual_loops
       */
      const nodeRegex = new RegExp(
        prefixRegex +
        typeRegex +
        '\\s*' +
        nonCapturingGroupOpen +
          (nonCapturingGroupOpen + estimationRegex + '\\s+' +
           openParenthesisRegex + actualRegex + closeParenthesisRegex +
           nonCapturingGroupClose) +
          '|' +
          nonCapturingGroupOpen + estimationRegex + nonCapturingGroupClose +
          '|' +
          nonCapturingGroupOpen + openParenthesisRegex + actualRegex + closeParenthesisRegex + nonCapturingGroupClose +
        nonCapturingGroupClose +
        '\\s*$',
        'gm',
      );
      const nodeMatches = nodeRegex.exec(line);

      // tslint:disable-next-line:max-line-length
      const subRegex = /^(\s*)((?:Sub|Init)Plan)\s*(?:\d+\s*)?\s*(?:\(returns.*\)\s*)?$/gm;
      const subMatches = subRegex.exec(line);

      const cteRegex = /^(\s*)CTE\s+(\S+)\s*$/g;
      const cteMatches = cteRegex.exec(line);

      /*
       * Groups
       * 2: trigger name
       * 3: time
       * 4: calls
       */
      const triggerRegex = /^(\s*)Trigger\s+(.*):\s+time=(\d+\.\d+)\s+calls=(\d+)\s*$/g;
      const triggerMatches = triggerRegex.exec(line);

      /*
       * Groups
       * 2: Worker number
       * 3: actual_time_first
       * 4: actual_time_last
       * 5: actual_rows
       * 6: actual_loops
       * 7: actual_rows_
       * 8: actual_loops_
       * 9: never_executed
       * 10: extra
       */
      const workerRegex = new RegExp(
        /^(\s*)Worker\s+(\d+):\s+/.source +
        nonCapturingGroupOpen +
        actualRegex +
        nonCapturingGroupClose +
        optionalGroup +
        '(.*)' +
        '\\s*$',
        'g',
      );
      const workerMatches = workerRegex.exec(line);

      const jitRegex = /^(\s*)JIT:\s*$/g;
      const jitMatches = jitRegex.exec(line);

      const extraRegex = /^(\s*)(\S.*\S)\s*$/g;
      const extraMatches = extraRegex.exec(line);

      if (emptyLineMatches || headerMatches) {
        return;
      } else if (nodeMatches && !cteMatches && !subMatches) {
        const prefix = nodeMatches[1];
        const neverExecuted = nodeMatches[13];
        const newNode: Node = new Node(nodeMatches[2]);
        if (nodeMatches[3] && nodeMatches[4] || nodeMatches[14] && nodeMatches[15]) {
          newNode[NodeProp.STARTUP_COST] = parseFloat(nodeMatches[3] || nodeMatches[14]);
          newNode[NodeProp.TOTAL_COST] = parseFloat(nodeMatches[4] || nodeMatches[15]);
          newNode[NodeProp.PLAN_ROWS] = parseInt(nodeMatches[5] || nodeMatches[16], 0);
          newNode[NodeProp.PLAN_WIDTH] = parseInt(nodeMatches[6] || nodeMatches[17], 0);
        }
        if (nodeMatches[7] && nodeMatches[8] || nodeMatches[18] && nodeMatches[19]) {
          newNode[NodeProp.ACTUAL_STARTUP_TIME] = parseFloat(nodeMatches[7] || nodeMatches[18]);
          newNode[NodeProp.ACTUAL_TOTAL_TIME] = parseFloat(nodeMatches[8] || nodeMatches[19]);
        }

        if (nodeMatches[9] && nodeMatches[10] || nodeMatches[11] && nodeMatches[12] ||
            nodeMatches[20] && nodeMatches[21]) {
          newNode[NodeProp.ACTUAL_ROWS] = parseInt(nodeMatches[9] || nodeMatches[11] || nodeMatches[20], 0);
          newNode[NodeProp.ACTUAL_LOOPS] = parseInt(nodeMatches[10] || nodeMatches[12] || nodeMatches[21], 0);
        }

        if (neverExecuted) {
          newNode[NodeProp.ACTUAL_LOOPS] = 0;
          newNode[NodeProp.ACTUAL_ROWS] = 0;
          newNode[NodeProp.ACTUAL_TOTAL_TIME] = 0;
        }
        const element = {
          node: newNode,
          subelementType: 'subnode',
        };

        if (0 === elementsAtDepth.length) {
          elementsAtDepth.push([depth, element]);
          root.Plan = newNode;
          return;
        }

        // Remove elements from elementsAtDepth for deeper levels
        _.remove(elementsAtDepth, (e) => {
          return e[0] >= depth;
        });

        // ! is for non-null assertion
        // Prevents the "Object is possibly 'undefined'" linting error
        const previousElement = _.last(elementsAtDepth)![1];

        elementsAtDepth.push([depth, element]);

        if (!previousElement.node[NodeProp.PLANS]) {
          previousElement.node[NodeProp.PLANS] = [];
        }
        if (previousElement.subelementType === 'initplan' ) {
          newNode[NodeProp.PARENT_RELATIONSHIP] = 'InitPlan';
          newNode[NodeProp.SUBPLAN_NAME] = previousElement.name;
        } else if (previousElement.subelementType === 'subplan' ) {
          newNode[NodeProp.PARENT_RELATIONSHIP] = 'SubPlan';
          newNode[NodeProp.SUBPLAN_NAME] = previousElement.name;
        }
        previousElement.node.Plans.push(newNode);

      } else if (subMatches) {
        const prefix = subMatches[1];
        const type = subMatches[2];
        // Remove elements from elementsAtDepth for deeper levels
        _.remove(elementsAtDepth, (e) => e[0] >= depth);
        const previousElement = _.last(elementsAtDepth)![1];
        const element = {
          node: previousElement.node,
          subelementType: type.toLowerCase(),
          name: subMatches[0],
        };
        elementsAtDepth.push([depth, element]);
      } else if (cteMatches) {
        const prefix = cteMatches[1];
        const cteName = cteMatches[2];
        // Remove elements from elementsAtDepth for deeper levels
        _.remove(elementsAtDepth, (e) => e[0] >= depth);
        const previousElement = _.last(elementsAtDepth)![1];
        const element = {
          node: previousElement.node,
          subelementType: 'initplan',
          name: 'CTE ' + cteName,
        };
        elementsAtDepth.push([depth, element]);
      } else if (workerMatches) {
        const prefix = workerMatches[1];
        const workerNumber = parseInt(workerMatches[2], 0);
        const previousElement = _.last(elementsAtDepth)![1];
        if (!previousElement.node[NodeProp.WORKERS]) {
          previousElement.node[NodeProp.WORKERS] = [];
        }
        let worker = this.getWorker(previousElement.node, workerNumber);
        if (!worker) {
          worker = new Worker(workerNumber);
          previousElement.node[NodeProp.WORKERS].push(worker);
        }
        if (workerMatches[3] && workerMatches[4]) {
          worker[NodeProp.ACTUAL_STARTUP_TIME] = parseFloat(workerMatches[3]);
          worker[NodeProp.ACTUAL_TOTAL_TIME] = parseFloat(workerMatches[4]);
          worker[NodeProp.ACTUAL_ROWS] = parseInt(workerMatches[5], 0);
          worker[NodeProp.ACTUAL_LOOPS] = parseInt(workerMatches[6], 0);
        }

        if (this.nodeHelper.parseSort(workerMatches[10], worker)) {
          return;
        }

        // extra info
        const info = workerMatches[10].split(/: (.+)/).filter((x) => x);
        if (workerMatches[10]) {
          if (!info[1]) {
            return;
          }
          const property = _.startCase(info[0]);
          worker[property] = info[1];
        }
      } else if (triggerMatches) {
        const prefix = triggerMatches[1];
        // Remove elements from elementsAtDepth for deeper levels
        _.remove(elementsAtDepth, (e) => e[0] >= depth);
        root.Triggers = root.Triggers || [];
        root.Triggers.push({
          'Trigger Name': triggerMatches[2],
          'Time': this.nodeHelper.parseTime(triggerMatches[3]),
          'Calls': triggerMatches[4],
        });
      } else if (jitMatches) {
        let element;
        if (elementsAtDepth.length === 0) {
          root.JIT = {};
          element = {
            node: root.JIT,
          };
          elementsAtDepth.push([1, element]);
        } else {
          const lastElement = _.last(elementsAtDepth)![1];
          if (_.last(lastElement.node[NodeProp.WORKERS])) {
            const worker: Worker = _.last(lastElement.node[NodeProp.WORKERS])! as Worker;
            worker.JIT = {};
            element = {
              node: worker.JIT,
            };
            elementsAtDepth.push([depth, element]);
          }
        }
      } else if (extraMatches) {
        const prefix = extraMatches[1];

        // Remove elements from elementsAtDepth for deeper levels
        _.remove(elementsAtDepth, (e) => e[0] >= depth);

        let element;
        if (elementsAtDepth.length === 0) {
          element = root;
        } else {
          element = _.last(elementsAtDepth)![1].node;
        }

        // if no node have been found yet and a 'Query Text' has been found
        // there the line is the part of the query
        if (!element.Plan && element['Query Text']) {
          element['Query Text'] += '\n' + line;
          return;
        }

        const info = extraMatches[2].split(/: (.+)/).filter((x) => x);
        if (!info[1]) {
          return;
        }

        if (this.nodeHelper.parseSort(extraMatches[2], element)) {
          return;
        }

        if (this.nodeHelper.parseBuffers(extraMatches[2], element)) {
          return;
        }

        if (this.nodeHelper.parseWAL(extraMatches[2], element)) {
          return;
        }

        if (this.nodeHelper.parseIOTimings(extraMatches[2], element)) {
          return;
        }

        if (this.nodeHelper.parseOptions(extraMatches[2], element)) {
          return;
        }

        if (this.nodeHelper.parseTiming(extraMatches[2], element)) {
          return;
        }

        if (this.nodeHelper.parseSettings(extraMatches[2], element)) {
          return;
        }

        if (this.nodeHelper.parseSortGroups(extraMatches[2], element)) {
          return;
        }

        if (this.nodeHelper.parseSortKey(extraMatches[2], element)) {
          return;
        }

        // remove the " ms" unit in case of time
        let value: string | number = info[1].replace(/(\s*ms)$/, '');
        // try to convert to number
        if (parseFloat(value)) {
          value = parseFloat(value);
        }

        let property = info[0];
        if (property.indexOf(' runtime') !== -1 || property.indexOf(' time') !== -1) {
          property = _.startCase(property);
        }
        element[property] = value;
      }
    });
    if (!root.Plan) {
      throw new Error('Unable to parse plan');
    }
    return root;
  }

  private getWorker(node: Node, workerNumber: number): Worker|null {
    return _.find(node[NodeProp.WORKERS], (worker) => {
      return worker[WorkerProp.WORKER_NUMBER] === workerNumber;
    });
  }










}
