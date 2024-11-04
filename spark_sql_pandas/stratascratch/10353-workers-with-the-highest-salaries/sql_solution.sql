SELECT
	T.WORKER_TITLE AS BEST_PAID_TITLE
FROM
	WORKER W
	JOIN TITLE T ON W.WORKER_ID = T.WORKER_REF_ID
WHERE
	SALARY = (
		SELECT
			MAX(SALARY)
		FROM
			WORKER
	)