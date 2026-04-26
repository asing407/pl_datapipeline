{{ config(materialized='table') }}

with source as (
    select * from {{ source('pl_pipeline', 'raw_matches') }}
),

cleaned as (
    select
        MATCH_ID,
        DATE::timestamp_ntz          as match_date,
        MATCHDAY                     as matchday,
        HOME_TEAM                    as home_team,
        AWAY_TEAM                    as away_team,
        HOME_GOALS                   as home_goals,
        AWAY_GOALS                   as away_goals,
        STATUS                       as status,
        VENUE                        as venue,
        REFEREE                      as referee,
        
        -- Derived columns
        HOME_GOALS + AWAY_GOALS         as total_goals,
        case
            when HOME_GOALS > AWAY_GOALS then HOME_TEAM
            when AWAY_GOALS > HOME_GOALS then AWAY_TEAM
            else 'Draw'
        end                             as winner,
        case
            when HOME_GOALS > AWAY_GOALS then 'Home Win'
            when AWAY_GOALS > HOME_GOALS then 'Away Win'
            else 'Draw'
        end                             as result_type
    from source
    where STATUS = 'Match Finished'
)

select * from cleaned