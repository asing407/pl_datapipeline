with matches as (
    select * from {{ ref('stg_matches') }}
)

select
    match_date,
    matchday,
    home_team,
    away_team,
    home_goals,
    away_goals,
    total_goals,
    winner,
    venue
from matches
where total_goals >= 4
order by total_goals desc, match_date desc