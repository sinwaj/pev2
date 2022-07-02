import { PlanService } from '@/services/plan-service';
import { SourceParser } from '@/services/sourceparser';

describe('PlanService', () => {
  test('correctly handles duplicated keys in JSON', () => {
    const planService = new PlanService();
    const source = `
[
  {
    "Plan": {
      "Workers": [
        {
          "Worker Number": 0,
          "Sort Method": "external merge",
          "Sort Space Used": 73552,
          "Sort Space Type": "Disk"
        },
        {
          "Worker Number": 1,
          "Sort Method": "external merge",
          "Sort Space Used": 73320,
          "Sort Space Type": "Disk"
        }
      ],
      "Workers": [
        {
          "Worker Number": 0,
          "Actual Startup Time": 1487.846,
          "Actual Total Time": 1996.879,
          "Actual Rows": 2692973,
          "Actual Loops": 1
        },
        {
          "Worker Number": 1,
          "Actual Startup Time": 1468.256,
          "Actual Total Time": 2012.744,
          "Actual Rows": 2684443,
          "Actual Loops": 1
        }
      ],
      "Plan Width": 36
    }
  }
]`;
    const json: any = new SourceParser().parseJson(source);
    expect(json.Plan.Workers.length).toEqual(2);
    expect(json.Plan.Workers[0]['Sort Method']).toEqual('external merge');
    expect(json.Plan.Workers[0]['Actual Startup Time']).toEqual(1487.846);
  });
});
