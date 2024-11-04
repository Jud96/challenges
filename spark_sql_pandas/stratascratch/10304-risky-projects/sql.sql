SELECT
	TITLE,
	BUDGET,
	CEILING(PRORATED_EXPENSES) AS PRORATED_EMPLOYEE_EXPENSE
FROM
	(
		SELECT
			TITLE,
			BUDGET,
			(END_DATE::DATE - START_DATE::DATE) * (SUM(SALARY) / 365) AS PRORATED_EXPENSES
		FROM
			LINKEDIN_PROJECTS A
			INNER JOIN LINKEDIN_EMP_PROJECTS B ON A.ID = B.PROJECT_ID
			INNER JOIN LINKEDIN_EMPLOYEES C ON B.EMP_ID = C.ID
		GROUP BY
			TITLE,
			BUDGET,
			END_DATE,
			START_DATE
	) A 
WHERE
	PRORATED_EXPENSES > BUDGET
ORDER BY
	TITLE ASC