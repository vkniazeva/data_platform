{% test price_consistency(model, min_col, avg_col, max_col) %}
select *
from {{ model }}
where {{ min_col }} > {{ avg_col }}
   or {{ avg_col }} > {{ max_col }}
   or {{ min_col }} > {{ max_col }}
{% endtest %}
