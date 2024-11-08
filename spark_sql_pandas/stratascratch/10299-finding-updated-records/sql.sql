SELECT DISTINCT ON (ID)
    ID,
	FIRST_NAME,
	LAST_NAME,
	DEPARTMENT_ID,
	MAX(SALARY) OVER (
		PARTITION BY
			ID
	) AS MAX
FROM
	MS_EMPLOYEE_SALARY E
ORDER BY
	ID ASC;