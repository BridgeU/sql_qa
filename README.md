# QA'ed SQL Queries

A repository containing vetted SQL queries that the data chapter signed-off on. 


1. Content recommendations query

The content recommendations query joins the `insights` & `analytics` datasets to obtain shortlisting data from a souce the contains common app
and parchment shortlists. The platform events data is used to pull profile views & article reads for data transformation.

The looker query below may assist in the qa, the numbers reflected in this report should tally with the counts for each of the selected tables.

https://looker.app.bridge-u.com/dashboards/402

```
-- all student country preferences for 2025
WITH country_preferences AS (SELECT c.official_name, a.global_id
    FROM insights.countries  AS c 
    LEFT JOIN insights.applications_countries  ac ON c.global_id = ac.global_country_id
    LEFT JOIN insights.applications  AS a on ac.global_application_id = a.global_id
    WHERE a.graduating_year = 2025),
-- all students in 2025 graduating year    
grad_2025 AS (
    SELECT
    students.analytics_id,
    applications.graduating_year,
    students.email,
    (CASE WHEN students.last_access_at > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 6 WEEK)  THEN 'Yes' ELSE 'No' END) AS students_active_last_six_weeks,
    DATE_DIFF( CAST(CONCAT(applications.graduating_year,'-','09-30') AS DATE), MIN(EXTRACT(DATE FROM students.last_access_at)) ,MONTH ) months_to_uni
    FROM insights.applications  AS applications 
    INNER JOIN insights.users  AS students ON applications.global_student_id = students.global_id AND students.role = 1
    INNER JOIN insights.schools  AS schools ON students.global_school_id = schools.global_id
    WHERE (NOT (applications.archived ) 
        OR (applications.archived ) IS NULL) 
        AND (applications.graduating_year ) = 2025 
        AND (UPPER(( students.email  )) NOT LIKE UPPER('%@bridge-u.com%') 
        AND UPPER(( students.email  )) NOT LIKE UPPER('%@bridgeu.com%') 
        OR ( students.email  ) IS NULL) 
        AND (schools.account_type ) = 0 
        AND (schools.active )
        AND (students.terms_accepted )
    GROUP BY 1,2,3,4),
-- subject relationships to map subject choices to subject area name    
top_subjects AS (
            SELECT subjects.global_id  AS id,
                    subjects.name AS area_name
            FROM insights.subjects AS subjects
            WHERE (subjects.top )), 
                  descendant_subjects AS (SELECT
                  subject_relationships.global_descendant_id  AS id,
                  subjects.name  AS area_name
            FROM insights.subjects  AS subjects
            INNER JOIN insights.subject_relationships  AS subject_relationships ON subjects.global_id = subject_relationships.global_ancestor_id
            WHERE (subjects.top )
            GROUP BY
                1,
                2),  
-- select distinct subjects & their descendent subjects
subject_areas AS (
      SELECT *
      FROM top_subjects
      UNION DISTINCT
      SELECT *
      FROM descendant_subjects),
-- aggregration of country & subject preferences alongside months_to_uni calculation
subject_country_preferences as (
    SELECT  students.analytics_id  AS users_analytics_id,
            DATE_DIFF( CAST(CONCAT(applications.graduating_year,'-','09-30') AS DATE), MIN(EXTRACT(DATE FROM students.last_access_at)) ,MONTH ) months_to_uni,
          ARRAY_AGG(DISTINCT country_preferences.official_name ) AS country_preferences,
          ARRAY_TO_STRING(array_agg(DISTINCT country_preferences.official_name ), ' | ' ) AS country_preferences_string,
          ARRAY_TO_STRING(ARRAY_AGG(distinct subject_interests_areas.area_name),' , ')  AS subject_interests_areas_subject_area_name,
          COUNT(CASE WHEN ( list_status = 1 ) THEN university_matches.global_id  ELSE NULL END) AS university_matches_count_shortlists,
    FROM insights.applications  AS applications
    LEFT OUTER JOIN insights.users  AS students ON applications.global_student_id = students.global_id AND students.role = 1
    INNER JOIN insights.schools  AS schools ON students.global_school_id = schools.global_id
    LEFT OUTER JOIN insights.university_matches  AS university_matches ON applications.global_id = university_matches.global_application_id
    INNER JOIN insights.countries  AS school_countries ON schools.country_code = school_countries.alpha2 and school_countries.source_db = 'com'
    INNER JOIN insights.profiles  AS profiles ON applications.global_id = profiles.global_application_id
    INNER  JOIN country_preferences ON applications.global_id = country_preferences.global_id
    INNER JOIN insights.subject_preferences  AS subject_preferences ON profiles.global_id = subject_preferences.global_profile_id
    INNER JOIN insights.subjects  AS subject_interests ON subject_preferences.global_subject_id = subject_interests.global_id
    LEFT JOIN subject_areas AS subject_interests_areas ON subject_interests.global_id = subject_interests_areas.id
    WHERE  (NOT (applications.archived ) -- student is not archived
            OR (applications.archived ) IS NULL) -- student is archived data is not null
            AND (applications.graduating_year ) = 2025 -- student is in 2025 graduating cohort
            AND (UPPER(( students.email  )) NOT LIKE UPPER('%@bridge-u.com%') --
            AND UPPER(( students.email  )) NOT LIKE UPPER('%@bridgeu.com%') 
            OR (students.email) IS NULL) 
            AND (schools.account_type ) = 0 
            AND (schools.active )
            AND (students.terms_accepted ) 
    GROUP BY 1,applications.graduating_year),
-- platform events pulled in for profile views    
platform_events_2025 AS (
      SELECT
          wh_platform__events_fact.event_user_natural_key  AS wh_platform__events_fact_event_user_natural_key,
          wh_core__students_dim.student_email,
          count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  then wh_platform__events_fact.event_pk end ) as sum_profile_views_only,

          count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  then wh_platform__events_fact.event_pk end ) as sum_partner_profile_views,
          
          count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name= 'united states of america' 
                  and country_name <> 'united kingdom' 
                  then wh_platform__events_fact.event_pk end ) as sum_us_partner_profile_views,
          
                  count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name <> 'united states of america' 
                  and country_name = 'united kingdom' 
                  then wh_platform__events_fact.event_pk end ) as sum_uk_partner_profile_views,
                  
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name <> 'united states of america' 
                  and country_name <> 'united kingdom' 
                  and country_name ='canada'
                  then wh_platform__events_fact.event_pk end ) as sum_cn_partner_profile_views,
               
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='switzerland'
                  then wh_platform__events_fact.event_pk end ) as sum_cz_partner_profile_views,
                
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='germany'
                  then wh_platform__events_fact.event_pk end ) as sum_de_partner_profile_views,
                  
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='france'
                  then wh_platform__events_fact.event_pk end ) as sum_fr_partner_profile_views,
                
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='italy'
                  then wh_platform__events_fact.event_pk end ) as sum_it_partner_profile_views,
              
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='ireland'
                  then wh_platform__events_fact.event_pk end ) as sum_ir_partner_profile_views,
                
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='finland'
                  then wh_platform__events_fact.event_pk end ) as sum_fn_partner_profile_views,
                
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='united arab emirates'
                  then wh_platform__events_fact.event_pk end ) as sum_uae_partner_profile_views,
                
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='grenada'
                  then wh_platform__events_fact.event_pk end ) as sum_gn_partner_profile_views,
              
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='spain'
                  then wh_platform__events_fact.event_pk end ) as sum_es_partner_profile_views,
                  
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status = 'customer' 
                  and country_name ='sweden'
                  then wh_platform__events_fact.event_pk end ) as sum_swed_partner_profile_views,
                  
                count (case when wh_platform__events_fact.event_name = 'university_overlay_tab_opened_server_side' 
                  and university_account_lifecycle_status <> 'customer'
                  then wh_platform__events_fact.event_pk end ) as sum_non_partner_profile_views
    FROM analytics.wh_platform__events_fact  AS wh_platform__events_fact 
    LEFT JOIN analytics.wh_core__students_dim  AS wh_core__students_dim ON wh_platform__events_fact.student_fk = wh_core__students_dim.student_pk
    LEFT JOIN analytics.wh_core__schools_dim  AS wh_core__schools_dim ON wh_platform__events_fact.school_fk = wh_core__schools_dim.school_pk
    LEFT JOIN analytics.wh_core__universities_dim  AS wh_core__event_universities_dim ON wh_platform__events_fact.university_fk = wh_core__event_universities_dim.university_pk
    LEFT JOIN analytics.wh_core__countries_dim  AS wh_core__university_countries_dim ON wh_core__university_countries_dim.country_pk = wh_core__event_universities_dim.country_fk
      WHERE ((UPPER(( wh_core__students_dim.student_email  )) NOT LIKE UPPER('%@bridgeu.com%') 
            AND UPPER(( wh_core__students_dim.student_email  )) NOT LIKE UPPER('%@bridge-u.com%') 
            OR ( wh_core__students_dim.student_email  ) IS NULL)) 
            AND ((UPPER(( wh_core__students_dim.student_archived  )) = UPPER('false'))) 
            AND (wh_core__students_dim.student_graduating_year ) = 2025 
            AND (wh_core__students_dim.student_terms_accepted ) 
            AND ((UPPER(( wh_core__schools_dim.school_active  )) = UPPER('true')))
      GROUP BY
          1,2
      ORDER BY
          1 DESC),
-- table with partner universities
uni_partners as (    
      SELECT wh_core__event_universities_dim.university_name  AS wh_core__event_universities_dim_university_name,
      wh_core__event_universities_dim.university_natural_key  AS wh_core__event_universities_dim_university_natural_key,
  FROM analytics.wh_platform__events_fact  AS wh_platform__events_fact
  LEFT JOIN analytics.wh_core__universities_dim  AS wh_core__event_universities_dim ON wh_platform__events_fact.university_fk = wh_core__event_universities_dim.university_pk
  WHERE ((UPPER(( wh_core__event_universities_dim.university_account_lifecycle_status  )) = UPPER('customer')))
  GROUP BY
      1,
      2
  ORDER BY 1),
-- insights shortlists
shortlists_2025 as (
    SELECT
        universities.buid  AS universities_buid,
        students.analytics_id  AS students_analytics_id,
         university_matches.global_id AS uni_match_id,
        
        COUNT(CASE WHEN ( list_status = 1 ) 
          THEN university_matches.global_id  
          ELSE NULL END) AS sum_shortlists,
          
        COUNT(CASE WHEN ( list_status = 1 ) 
                  THEN students.analytics_id  
                  ELSE NULL END) AS shortlists,
                  
        COUNT(CASE WHEN ( list_status = 1 ) 
                  AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
                  THEN  students.analytics_id 
                  ELSE NULL END) AS partner_shortlists,
                  
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
            THEN  university_matches.global_id  
            ELSE NULL END) AS sum_partner_shortlists,
            
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'United Kingdom' 
             and university_countries.name <> 'United States of America'   
            THEN  university_matches.global_id  END) AS sum_uk_partner_shortlists,
      
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name <> 'United Kingdom' 
             and university_countries.name = 'United States of America'   
            THEN  university_matches.global_id  
             END) AS sum_us_partner_shortlists,
  
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'Canada'   
            THEN  university_matches.global_id  
             END) AS sum_ca_partner_shortlists,
      
        
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'Switzerland'   
            THEN  university_matches.global_id  
             END) AS sum_cz_partner_shortlists,
    
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'Germany'   
            THEN  university_matches.global_id  
             END) AS sum_de_partner_shortlists,
     
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'France'   
            THEN  university_matches.global_id  
             END) AS sum_fr_partner_shortlists,
    
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'Italy'   
            THEN  university_matches.global_id  
             END) AS sum_it_partner_shortlists,
      
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'United Arab Emirates'   
            THEN  university_matches.global_id  
             END) AS sum_uae_partner_shortlists,
      
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'Finland'   
            THEN  university_matches.global_id  
             END) AS sum_fn_partner_shortlists,
       
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'Ireland'   
            THEN  university_matches.global_id  
             END) AS sum_ir_partner_shortlists,
       
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'Grenada'   
            THEN  university_matches.global_id  
             END) AS sum_gn_partner_shortlists,
             
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'Spain'   
            THEN  university_matches.global_id  
             END) AS sum_es_partner_shortlists,
             
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
             and university_countries.name = 'Sweden'   
            THEN  university_matches.global_id  
             END) AS sum_sw_partner_shortlists,
      
      COUNT(CASE WHEN ( list_status = 1 ) 
            AND universities.buid NOT IN (SELECT wh_core__event_universities_dim_university_natural_key FROM uni_partners)
            THEN  university_matches.global_id  
             END) AS sum_non_partner_shortlists,
      FROM insights.applications  AS applications 
      INNER JOIN insights.users  AS students ON applications.global_student_id = students.global_id AND students.role = 1
      INNER JOIN insights.schools  AS schools ON students.global_school_id = schools.global_id
      LEFT JOIN insights.university_matches  AS university_matches ON applications.global_id = university_matches.global_application_id
      LEFT JOIN insights.universities  AS universities ON university_matches.global_university_id = universities.global_id
      LEFT JOIN insights.countries  AS university_countries ON universities.country_code = university_countries.alpha2 AND universities.source_db = university_countries.source_db  
      WHERE (NOT (applications.archived ) 
            OR (applications.archived ) IS NULL) 
            AND ((applications.graduating_year ) = 2025 
            AND ((UPPER(( students.email  )) NOT LIKE UPPER('%@bridge-u.com%') 
            AND UPPER(( students.email  )) NOT LIKE UPPER('%@bridgeu.com%') 
            OR (students.email) IS NULL))) 
            AND ((students.terms_accepted ) 
            AND ((schools.account_type ) = 0 
            AND (schools.active ))) 
            AND (list_status =1 )
    GROUP BY
        1,
        2,
        3),
-- article engagement for students who have read a full article, saved an article or clicked an article        
article_engagement_2025 as (
  SELECT wh_platform__events_fact.event_user_natural_key  AS wh_platform__events_fact_event_user_natural_key,
      array_agg(coalesce(wh_marketing__articles_dim.article_title,'blank'))  AS wh_marketing__articles_dim_article_natural_key,
      COUNT(DISTINCT wh_platform__events_fact.event_pk ) AS wh_platform__events_fact_count_of_event_pk,
      COUNT(DISTINCT wh_platform__events_fact.event_user_natural_key ) AS count_of_event_user_natural_key
  FROM analytics.wh_platform__events_fact  AS wh_platform__events_fact
  LEFT JOIN analytics.wh_core__students_dim  AS wh_core__students_dim ON wh_platform__events_fact.student_fk = wh_core__students_dim.student_pk
  LEFT JOIN analytics.wh_marketing__articles_dim  AS wh_marketing__articles_dim ON wh_marketing__articles_dim.article_pk = wh_platform__events_fact.article_fk
  WHERE ((UPPER(( wh_platform__events_fact.event_name  )) <> UPPER('engage_student_read_full_article') 
    AND UPPER(( wh_platform__events_fact.event_name  )) <> UPPER('engage_student_save_article') 
    AND UPPER(( wh_platform__events_fact.event_name  )) <> UPPER('engage_article_link_click_server_side') 
    AND UPPER(( wh_platform__events_fact.event_name  )) <> UPPER('engage_student_clicked_article_apply_cta') 
    -- AND UPPER(( wh_platform__events_fact.event_name  )) <> UPPER('page_video_play') 
    OR ( wh_platform__events_fact.event_name  ) IS NULL)) 
    AND ((UPPER(( wh_platform__events_fact.event_product_group  )) = UPPER('articles'))) 
    AND (wh_core__students_dim.student_graduating_year ) = 2025
  GROUP BY 1
  ORDER BY 1 DESC)
-- data transformations and joins        
SELECT 
    platform_events_2025.wh_platform__events_fact_event_user_natural_key,
    grad_2025.months_to_uni,
    students_active_last_six_weeks,
    (CASE WHEN 'United Kingdom' IN UNNEST(subject_country_preferences.country_preferences) 
          AND 'United States' NOT IN UNNEST(subject_country_preferences.country_preferences)
          THEN 1 ELSE 0 END) AS uk_only,
          
    (CASE WHEN 'United Kingdom' NOT IN UNNEST(subject_country_preferences.country_preferences) 
          AND 'United States' IN UNNEST(subject_country_preferences.country_preferences) 
          THEN 1 ELSE 0 END) AS us_only,
                
    (CASE WHEN 'United Kingdom' IN UNNEST(subject_country_preferences.country_preferences) 
          AND 'United States' IN UNNEST(subject_country_preferences.country_preferences) 
          THEN 1 ELSE 0 END ) AS us_us_both,
                
    count(distinct case when shortlists_2025.sum_shortlists = 1 
                then uni_match_id end) as sum_shortlists,

    count(distinct case when shortlists_2025.sum_partner_shortlists = 1 
                then uni_match_id end) as sum_partner_shortlists,
                
    count(distinct case when shortlists_2025.sum_non_partner_shortlists = 1 
                then uni_match_id end) as sum_non_partner_shortlists,

    count(distinct case when shortlists_2025.sum_uk_partner_shortlists = 1 
                then uni_match_id end) as sum_uk_partner_shortlists,
                
    count(distinct case when shortlists_2025.sum_us_partner_shortlists= 1
                then uni_match_id end) as sum_us_partner_shortlists,

    count(distinct case when sum_ca_partner_shortlists= 1
                then uni_match_id end) as sum_ca_partner_shortlists,
      
    count(distinct case when shortlists_2025.sum_cz_partner_shortlists= 1
                then uni_match_id end) as sum_cz_partner_shortlists,     
                
    count(distinct case when shortlists_2025.sum_de_partner_shortlists = 1
                then uni_match_id end) as sum_de_partner_shortlists,         

    count(distinct case when shortlists_2025.sum_fr_partner_shortlists = 1
                then uni_match_id end) as sum_fr_partner_shortlists,    
                
    count(distinct case when shortlists_2025.sum_it_partner_shortlists = 1
                then uni_match_id end) as sum_it_partner_shortlists,    
                
    count(distinct case when shortlists_2025.sum_uae_partner_shortlists = 1
                then uni_match_id end) as sum_uae_partner_shortlists, 

    count(distinct case when shortlists_2025.sum_fn_partner_shortlists = 1
                then uni_match_id end) as sum_fn_partner_shortlists,    

    count(distinct case when shortlists_2025.sum_ir_partner_shortlists = 1
                then uni_match_id end) as sum_ir_partner_shortlists,    

    count(distinct case when shortlists_2025.sum_gn_partner_shortlists = 1
                then uni_match_id end) as sum_gn_partner_shortlists,    

    count(distinct case when shortlists_2025.sum_sw_partner_shortlists = 1
                then uni_match_id end) as sum_sw_partner_shortlists,    
sum_profile_views_only,
sum_partner_profile_views,
sum_us_partner_profile_views,
sum_uk_partner_profile_views,
sum_cn_partner_profile_views,
sum_cz_partner_profile_views,
sum_de_partner_profile_views,
sum_fr_partner_profile_views,
sum_it_partner_profile_views,
sum_ir_partner_profile_views,
sum_fn_partner_profile_views ,
sum_uae_partner_profile_views, 
sum_gn_partner_profile_views,
sum_es_partner_profile_views,
sum_swed_partner_profile_views,
sum_non_partner_profile_views,
subject_interests_areas_subject_area_name,
ARRAY_TO_STRING(wh_marketing__articles_dim_article_natural_key,' || ') as articles_read
    FROM platform_events_2025 
    -- left outer joins are used to include all students who despite their engagement with articles, profile views & shortlists
    LEFT OUTER JOIN grad_2025 on grad_2025.analytics_id = platform_events_2025.wh_platform__events_fact_event_user_natural_key and grad_2025.email = platform_events_2025.student_email 
    LEFT OUTER JOIN subject_country_preferences ON grad_2025.analytics_id = subject_country_preferences.users_analytics_id
    LEFT JOIN shortlists_2025 ON platform_events_2025.wh_platform__events_fact_event_user_natural_key = shortlists_2025.students_analytics_id
    LEFT JOIN article_engagement_2025 ON grad_2025.analytics_id = article_engagement_2025.wh_platform__events_fact_event_user_natural_key
GROUP BY platform_events_2025.wh_platform__events_fact_event_user_natural_key,
            grad_2025.months_to_uni, uk_only, us_only, us_us_both, 
            sum_partner_profile_views, sum_profile_views_only, subject_interests_areas_subject_area_name, articles_read, 
            sum_us_partner_profile_views, sum_uk_partner_profile_views, sum_cn_partner_profile_views, 
            sum_cz_partner_profile_views, sum_de_partner_profile_views, sum_fr_partner_profile_views, sum_it_partner_profile_views,
            sum_ir_partner_profile_views, sum_fn_partner_profile_views, sum_uae_partner_profile_views, 
            sum_gn_partner_profile_views, sum_es_partner_profile_views, sum_swed_partner_profile_views, sum_non_partner_profile_views,
            students_active_last_six_weeks

```

""Our philosophy QA once re-use, safely re-use."""

