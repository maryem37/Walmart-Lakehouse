{% test greater_than(model, column_name, value) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} <= {{ value }}
   OR {{ column_name }} IS NULL

{% endtest %}