SELECT candidate
FROM
  (SELECT candidate,
          round(sum(vote_value), 3) n_votes,
          dense_rank() over(
                            ORDER BY round(sum(vote_value), 3) DESC) place
   FROM
     (SELECT voter,
             candidate,
             1.0 / count(*) over(PARTITION BY voter) vote_value
      FROM voting_results
      WHERE candidate IS NOT NULL)a
   GROUP BY candidate)results
WHERE place = 1