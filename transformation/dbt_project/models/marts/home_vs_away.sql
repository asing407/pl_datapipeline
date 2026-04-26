with matches as (
    select * from {{ ref('stg_matches') }}
)

select
    result_type,
    count(*)                                    as total_matches,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage,
    avg(total_goals)                            as avg_goals
from matches
group by result_type
order by total_matches desc