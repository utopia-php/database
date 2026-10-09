<?php

namespace Utopia\Database\Builder;

use Utopia\Database\Database;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Validator\ObjectPath;
use Utopia\Query\Builder\PostgreSQL as Base;
use Utopia\Query\Builder\SpatialDistanceFilter;
use Utopia\Query\Method;
use Utopia\Query\Query;
use Utopia\Query\Schema\ColumnType;

/**
 * The PostgreSQL builder, which also compiles filters on their own, prepares and matches search terms as 7.x did,
 * writes a filter on a path into an object attribute only when every key of the path is a plain key, and lets a
 * distanceLessThan() filter use the spatial index.
 */
class Postgres extends Base implements Filtering, Scoping
{
    use CompilesFilters;
    use PreparesSearchTerms;
    use ScopesCollections;

    private const float METERS_PER_LATITUDE_DEGREE = 110574;

    private const float METERS_PER_EQUATORIAL_LONGITUDE_DEGREE = 111319;

    /**
     * @throws QueryException On a builder that read a collection, whose tenant scope does not reach the second table
     */
    #[\Override]
    public function updateFrom(string $table, string $alias = ''): static
    {
        $this->requireUnbound("update from table '{$table}'");

        return parent::updateFrom($table, $alias);
    }

    /**
     * @throws QueryException On a builder that read a collection, whose tenant scope does not reach the second table
     */
    #[\Override]
    public function deleteUsing(string $table, string $condition, mixed ...$bindings): static
    {
        $this->requireUnbound("delete using table '{$table}'");

        return parent::deleteUsing($table, $condition, ...$bindings);
    }

    /**
     * 7.x handed websearch_to_tsquery() an exact term in single quotes, which match every word in any order where
     * double quotes would match the adjacent phrase, and any other term with its words joined by `or`.
     *
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileSearchExpression(string $attribute, array $values, bool $not): string
    {
        $term = $values[0] ?? '';
        $term = \is_string($term) ? $term : '';
        $words = $this->searchWords($term);

        if ($words === '') {
            return $not ? '1 = 1' : '1 = 0';
        }

        $this->addBinding("'".($this->isExactSearch($term) ? $words : \str_replace(' ', ' or ', $words))."'");
        $match = "to_tsvector(regexp_replace({$attribute}, '[^\\w]+', ' ', 'g')) @@ websearch_to_tsquery(?)";

        return $not ? "NOT ({$match})" : $match;
    }

    /**
     * @throws QueryException
     */
    #[\Override]
    public function compileFilter(Query $query): string
    {
        $attribute = $query->getAttribute();

        if ($query->getAttributeType() === ColumnType::Object->value && \str_contains($attribute, '.')) {
            $path = new ObjectPath();
            if (! $path->isValid($attribute)) {
                throw new QueryException('Invalid object path "'.$attribute.'": '.$path->getDescription());
            }
        }

        return parent::compileFilter($query);
    }

    /**
     * ST_Distance() cannot be served by the GIST index, so distanceLessThan() leads with a predicate that can: ST_DWithin()
     * on the geometry, or, for a distance in meters from a point to a point column, a degree box around the point. The
     * exact ST_Distance() check follows in every case and keeps the boundary exclusive.
     */
    #[\Override]
    protected function compileSpatialFilter(Method $method, string $attribute, Query $query): string
    {
        if ($method !== Method::DistanceLessThan) {
            return parent::compileSpatialFilter($method, $attribute, $query);
        }

        /** @var array{0: string|array<mixed>, 1: float, 2: bool} $tuple */
        $tuple = $query->getValues()[0];
        $filter = SpatialDistanceFilter::fromTuple($tuple);
        $wkt = \is_array($filter->geometry) ? $this->geometryToWkt($filter->geometry) : $filter->geometry;
        $geometry = 'ST_GeomFromText(?, '.Database::DEFAULT_SRID.')';

        if (! $filter->meters) {
            $this->addBinding($wkt);
            $this->addBinding($filter->distance);
            $this->addBinding($wkt);
            $this->addBinding($filter->distance);

            return '(ST_DWithin('.$attribute.', '.$geometry.', ?) AND ST_Distance('.$attribute.', '.$geometry.') < ?)';
        }

        $distance = 'ST_Distance(('.$attribute.'::geography), ST_SetSRID(ST_GeomFromText(?), '.Database::DEFAULT_SRID.')::geography) < ?';
        $degrees = $query->getAttributeType() === ColumnType::Point->value
            ? self::degreesWithinMeters($filter->geometry, $filter->distance)
            : null;

        if ($degrees === null) {
            $this->addBinding($wkt);
            $this->addBinding($filter->distance);

            return $distance;
        }

        $this->addBinding($wkt);
        $this->addBinding($degrees[0]);
        $this->addBinding($degrees[1]);
        $this->addBinding($wkt);
        $this->addBinding($filter->distance);

        return '('.$attribute.' && ST_Expand('.$geometry.', ?, ?) AND '.$distance.')';
    }

    /**
     * The longitude and latitude degrees that hold every point within $meters of $point on the WGS84 spheroid, where a
     * degree of latitude spans at least 110,574 m and a degree of longitude at least 111,319 m × cos(latitude).
     *
     * Null for a line or polygon, whose geodesic edges leave any degree box, for a distance that is not finite, and
     * when the box would reach a pole or the antimeridian.
     *
     * @param  string|array<mixed>  $point
     * @return array{0: float, 1: float}|null
     */
    private static function degreesWithinMeters(string|array $point, float $meters): ?array
    {
        if (! \is_finite($meters) || ! \is_array($point) || \count($point) !== 2 || ! \is_numeric($point[0] ?? null) || ! \is_numeric($point[1] ?? null)) {
            return null;
        }

        $longitude = (float) $point[0];
        $latitude = (float) $point[1];

        $latitudeDegrees = $meters / self::METERS_PER_LATITUDE_DEGREE;
        if (\abs($latitude) + $latitudeDegrees >= 90) {
            return null;
        }

        $longitudeDegrees = $meters / (self::METERS_PER_EQUATORIAL_LONGITUDE_DEGREE * \cos(\deg2rad(\abs($latitude) + $latitudeDegrees)));
        if ($longitude - $longitudeDegrees <= -180 || $longitude + $longitudeDegrees >= 180) {
            return null;
        }

        return [$longitudeDegrees, $latitudeDegrees];
    }
}
