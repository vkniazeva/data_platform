{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as cnt
from {{ model }}
group by {{ columns | join(', ') }}
having cnt > 1
{% endtest %}
