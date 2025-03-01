{% test assert_positive_value(model, column_name) %}
Select *
from {{model}}
where {{column_name}}<0
{% endtest %}