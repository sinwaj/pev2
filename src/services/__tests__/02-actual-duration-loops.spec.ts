import { PlanService } from '@/services/plan-service';
import { IPlan } from '@/iplan';


describe('PlanService', () => {
  test('Actual durations are correct', () => {
    const planService = new PlanService();
    const source = `
Gather Motion 2:1  (slice1; segments: 2)  (cost=0.00..431.00 rows=1 width=8)
   ->  Sequence  (cost=0.00..431.00 rows=1 width=8)
         ->  Partition Selector for sales (dynamic scan id: 1)  (cost=10.00..100.00 rows=50 width=4)
               Filter: year = 2015
               Partitions selected:  1 (out of 100)
         ->  Dynamic Table Scan on sales (dynamic scan id: 1)  (cost=0.00..431.00 rows=1 width=8)
               Filter: year = 2015
 Settings:  optimizer=on
 Optimizer status: PQO version 1.620
(9 rows)
`;


    const r: any = planService.fromSource(source);
    const plan: IPlan = planService.createPlan('', r, '');
    // Materialize duration: total time * loops - Seq Scan duration
    const mDuration = 0.008 * 402 - 0.015;
    expect(plan.content.Plan.Plans[1]['*Duration (exclusive)']).toBe(mDuration);

    // Nested Loop duration: total time - (Materialize duration + Seq Scan
    // duration) - Seq Scan duration
    const nlDuration = 10.198 - (mDuration + 0.015) - 0.058;
    expect(plan.content.Plan['*Duration (exclusive)']).toBe(nlDuration);
  });
});
