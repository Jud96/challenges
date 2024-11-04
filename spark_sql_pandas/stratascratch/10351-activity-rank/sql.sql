SELECT 
    from_user,
    total_emails,
    Row_number() OVER (ORDER BY total_emails DESC,from_user asc ) AS row_number
FROM (
    SELECT 
        from_user,
        COUNT(*) AS total_emails
    FROM 
        google_gmail_emails
    GROUP BY 
        from_user
) AS user_email_counts