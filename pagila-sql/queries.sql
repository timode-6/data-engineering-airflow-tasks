-- Task 1. Print the number of films in each category, -- sort in descending order
SELECT c.name AS category, COUNT(fc.film_id) AS films
FROM category c
LEFT JOIN film_category fc ON fc.category_id = c.category_id
GROUP BY c.category_id, c.name
ORDER BY films DESC, c.name;

-- Task 2. Bring out the 10 actors whose films have been rented the most, -- sort in descending order
SELECT a.actor_id, a.first_name, a.last_name, COUNT(*) AS rentals
FROM actor a
JOIN film_actor fa ON fa.actor_id = a.actor_id
JOIN inventory i ON i.film_id = fa.film_id
JOIN rental r ON r.inventory_id = i.inventory_id
GROUP BY a.actor_id, a.first_name, a.last_name
ORDER BY rentals DESC, a.actor_id
LIMIT 10;


-- Task 3. Show the category of films that you spent the most money on
SELECT c.name AS category, SUM(p.amount) AS revenue
FROM category c
JOIN film_category fc ON fc.category_id = c.category_id
JOIN inventory i ON i.film_id = fc.film_id
JOIN rental r ON r.inventory_id = i.inventory_id
JOIN payment p ON p.rental_id = r.rental_id
GROUP BY c.category_id, c.name
ORDER BY revenue DESC
FETCH FIRST 1 ROW WITH TIES;

-- Task 4. Print movie titles that are not in the inventory. Write a query without using the IN operator
SELECT f.film_id, f.title
FROM film f
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.film_id = f.film_id)
ORDER BY f.title;

-- Task 5. Bring out the top 3 actors who have been appeared in films the most
-- in the 'Children' category. If several actors have the same number of films, bring them all out
-- "Top 3" here means three rows plus every actor tied with the third
-- (FETCH FIRST 3 ROWS WITH TIES, equivalently RANK() <= 3): 5 rows on this
-- data — one actor with 7 films, four tied on 5. The alternative reading,
-- the top three distinct film counts (DENSE_RANK() <= 3), returns more.
SELECT a.actor_id, a.first_name, a.last_name, COUNT(*) AS films
FROM actor a
JOIN film_actor fa ON fa.actor_id = a.actor_id
JOIN film_category fc ON fc.film_id = fa.film_id
JOIN category c ON c.category_id = fc.category_id
WHERE c.name = 'Children'
GROUP BY a.actor_id, a.first_name, a.last_name
ORDER BY films DESC
FETCH FIRST 3 ROWS WITH TIES;

-- Task 6. Display cities with the number of active and inactive customers (active - customer.active = 1).
-- Sort by quantity inactive customers in descending order
SELECT ci.city, COUNT(*) FILTER (WHERE cu.active = 1) AS active_customers, COUNT(*) FILTER (WHERE cu.active = 0) AS inactive_customers, COUNT(*) AS total_customers
FROM city ci
JOIN address a ON a.city_id = ci.city_id
JOIN customer cu ON cu.address_id = a.address_id
GROUP BY ci.city_id, ci.city
ORDER BY inactive_customers DESC, ci.city;


-- Task 7. Display the movie category with the most hours total rent in cities (customer.address_id in the city),
-- starting with the letter "a". The same is true for cities that have the '-' symbol. All in one request
WITH rental_hours AS (
    SELECT c.name AS category, ci.city, EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600.0 AS hours
    FROM rental r
    JOIN inventory i ON i.inventory_id = r.inventory_id
    JOIN film_category fc ON fc.film_id = i.film_id
    JOIN category c ON c.category_id = fc.category_id
    JOIN customer cu ON cu.customer_id = r.customer_id
    JOIN address a ON a.address_id = cu.address_id
    JOIN city ci ON ci.city_id = a.city_id
    WHERE r.return_date IS NOT NULL
),
per_group AS (
    SELECT 'cities starting with "a"' AS city_group, category, SUM(hours) AS total_hours
    FROM rental_hours
    WHERE city ILIKE 'a%'
    GROUP BY category

    UNION ALL

    SELECT 'cities containing "-"' AS city_group, category, SUM(hours) AS total_hours
    FROM rental_hours
    WHERE city LIKE '%-%'
    GROUP BY category
),
ranked AS (
    SELECT city_group, category, total_hours,
    RANK() OVER (PARTITION BY city_group
    ORDER BY total_hours DESC) AS rnk
    FROM per_group
)
SELECT city_group, category, ROUND(total_hours::numeric, 2) AS total_rental_hours
FROM ranked
WHERE rnk = 1
ORDER BY city_group;
