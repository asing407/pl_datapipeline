with match_results as (
    select * from {{ ref('stg_matches') }}
),

home_stats as (
    select
        home_team                                    as team,
        count(*)                                     as home_played,
        sum(case when winner = home_team then 1 else 0 end) as home_wins,
        sum(case when result_type = 'Draw' then 1 else 0 end) as home_draws,
        sum(case when winner = away_team then 1 else 0 end) as home_losses,
        sum(home_goals)                              as home_goals_scored,
        sum(away_goals)                              as home_goals_conceded
    from match_results
    group by home_team
),

away_stats as (
    select
        away_team                                    as team,
        count(*)                                     as away_played,
        sum(case when winner = away_team then 1 else 0 end) as away_wins,
        sum(case when result_type = 'Draw' then 1 else 0 end) as away_draws,
        sum(case when winner = home_team then 1 else 0 end) as away_losses,
        sum(away_goals)                              as away_goals_scored,
        sum(home_goals)                              as away_goals_conceded
    from match_results
    group by away_team
),

combined as (
    select
        h.team,
        h.home_played + a.away_played                as total_played,
        h.home_wins + a.away_wins                    as total_wins,
        h.home_draws + a.away_draws                  as total_draws,
        h.home_losses + a.away_losses                as total_losses,
        h.home_goals_scored + a.away_goals_scored    as goals_scored,
        h.home_goals_conceded + a.away_goals_conceded as goals_conceded,
        (h.home_goals_scored + a.away_goals_scored) -
        (h.home_goals_conceded + a.away_goals_conceded) as goal_difference,
        (h.home_wins + a.away_wins) * 3 +
        (h.home_draws + a.away_draws)                as total_points
    from home_stats h
    join away_stats a on h.team = a.team
)

select
    rank() over (order by total_points desc, goal_difference desc) as position,
    team,
    total_played,
    total_wins,
    total_draws,
    total_losses,
    goals_scored,
    goals_conceded,
    goal_difference,
    total_points
from combined
order by position