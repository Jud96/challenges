select f.policy_num,f.state,f.claim_cost,f.fraud_score from 
(select
    state,
  percentile_cont(0.95) within group (order by fraud_score asc) as top_5_pct_by_state
from fraud_score
group by state)t join fraud_score f on t.state = f.state
where t.top_5_pct_by_state <= f.fraud_score;
