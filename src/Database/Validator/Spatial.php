<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Validator;

class Spatial extends Validator
{
    private string $spatialType;
    protected string $message = '';

    public function __construct(string $spatialType)
    {
        $this->spatialType = $spatialType;
    }

    /**
     * Validate POINT data
     *
     * @param array<mixed> $value
     * @return bool
     */
    protected function validatePoint(array $value): bool
    {
        if (count($value) !== 2) {
            $this->message = 'Point must be an array of two numeric values [x, y]';
            return false;
        }

        if (!is_numeric($value[0]) || !is_numeric($value[1])) {
            $this->message = 'Point coordinates must be numeric values';
            return false;
        }

        return true;
    }

    /**
     * Validate LINESTRING data
     *
     * @param array<mixed> $value
     * @return bool
     */
    protected function validateLineString(array $value): bool
    {
        if (count($value) < 2) {
            $this->message = 'LineString must contain at least two points';
            return false;
        }

        foreach ($value as $point) {
            if (!is_array($point) || count($point) !== 2) {
                $this->message = 'Each point in LineString must be an array of two values [x, y]';
                return false;
            }

            if (!is_numeric($point[0]) || !is_numeric($point[1])) {
                $this->message = 'Each point in LineString must have numeric coordinates';
                return false;
            }
        }

        return true;
    }

    /**
     * Validate POLYGON data
     *
     * @param array<mixed> $value
     * @return bool
     */
    protected function validatePolygon(array $value): bool
    {
        if (empty($value)) {
            $this->message = 'Polygon must contain at least one ring';
            return false;
        }

        // Detect single-ring polygon: [[x, y], [x, y], ...]
        $isSingleRing = isset($value[0]) && is_array($value[0]) &&
            count($value[0]) === 2 &&
            is_numeric($value[0][0]) &&
            is_numeric($value[0][1]);

        if ($isSingleRing) {
            $value = [$value]; // wrap single ring
        }

        foreach ($value as $ringIndex => $ring) {
            if (!is_array($ring) || empty($ring)) {
                $this->message = "Ring #{$ringIndex} must be an array of points";
                return false;
            }

            if (count($ring) < 4) {
                $this->message = "Ring #{$ringIndex} must contain at least 4 points to form a closed polygon";
                return false;
            }

            foreach ($ring as $pointIndex => $point) {
                if (!is_array($point) || count($point) !== 2) {
                    $this->message = "Point #{$pointIndex} in ring #{$ringIndex} must be an array of two values [x, y]";
                    return false;
                }

                if (!is_numeric($point[0]) || !is_numeric($point[1])) {
                    $this->message = "Coordinates of point #{$pointIndex} in ring #{$ringIndex} must be numeric";
                    return false;
                }
            }

            // Check that the ring is closed (first point == last point)
            if ($ring[0] !== $ring[count($ring) - 1]) {
                $this->message = "Ring #{$ringIndex} must be closed (first point must equal last point)";
                return false;
            }
        }

        return true;
    }

    /**
     * Check if a value is valid WKT string
     */
    public static function isWKTString(string $value): bool
    {
        $value = trim($value);
        return (bool) preg_match('/^(POINT|LINESTRING|POLYGON)\s*\(/i', $value);
    }

    public function getDescription(): string
    {
        return 'Value must be a valid ' . $this->spatialType . ": {$this->message}";
    }

    public function isArray(): bool
    {
        return true;
    }

    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }

    /**
     * Main validation entrypoint
     */
    public function isValid($value): bool
    {
        if (is_null($value)) {
            return true;
        }

        if (is_string($value)) {
            return self::isWKTString($value);
        }

        if (is_array($value)) {
            switch ($this->spatialType) {
                case Database::VAR_POINT:
                    return $this->validatePoint($value);

                case Database::VAR_LINESTRING:
                    return $this->validateLineString($value);

                case Database::VAR_POLYGON:
                    return $this->validatePolygon($value);

                default:
                    $this->message = 'Unknown spatial type: ' . $this->spatialType;
                    return false;
            }
        }

        $this->message = 'Spatial value must be array or WKT string';
        return false;
    }
}
