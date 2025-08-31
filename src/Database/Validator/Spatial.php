<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Database;
use Utopia\Database\Exception;
use Utopia\Validator;

class Spatial extends Validator
{
    private string $spatialType;

    public function __construct(string $spatialType)
    {
        $this->spatialType = $spatialType;
    }

    /**
     * Validate spatial data according to its type
     *
     * @param mixed $value
     * @param string $type
     * @return bool
     * @throws Exception
     */
    public static function validate(mixed $value, string $type): bool
    {
        if (!is_array($value)) {
            throw new Exception('Spatial data must be provided as an array');
        }

        switch ($type) {
            case Database::VAR_POINT:
                return self::validatePoint($value);

            case Database::VAR_LINESTRING:
                return self::validateLineString($value);

            case Database::VAR_POLYGON:
                return self::validatePolygon($value);

            default:
                throw new Exception('Unknown spatial type: ' . $type);
        }
    }

    /**
     * Validate POINT data
     *
     * @param array<mixed,mixed> $value
     * @return bool
     * @throws Exception
     */
    protected static function validatePoint(array $value): bool
    {
        if (count($value) !== 2) {
            throw new Exception('Point must be an array of two numeric values [x, y]');
        }

        if (!is_numeric($value[0]) || !is_numeric($value[1])) {
            throw new Exception('Point coordinates must be numeric values');
        }

        return true;
    }

    /**
     * Validate LINESTRING data
     *
     * @param array<mixed,mixed> $value
     * @return bool
     * @throws Exception
     */
    protected static function validateLineString(array $value): bool
    {
        if (count($value) < 2) {
            throw new Exception('LineString must contain at least one point');
        }

        foreach ($value as $point) {
            if (!is_array($point) || count($point) !== 2) {
                throw new Exception('Each point in LineString must be an array of two values [x, y]');
            }

            if (!is_numeric($point[0]) || !is_numeric($point[1])) {
                throw new Exception('Each point in LineString must have numeric coordinates');
            }
        }

        return true;
    }

    /**
     * Validate POLYGON data
     *
     * @param array<mixed,mixed> $value
     * @return bool
     * @throws Exception
     */
    protected static function validatePolygon(array $value): bool
    {
        if (empty($value)) {
            throw new Exception('Polygon must contain at least one ring');
        }

        // Detect single-ring polygon: [[x, y], [x, y], ...]
        $isSingleRing = isset($value[0]) && is_array($value[0]) &&
                        count($value[0]) === 2 && is_numeric($value[0][0]) && is_numeric($value[0][1]);

        if ($isSingleRing) {
            $value = [$value]; // Wrap single ring into multi-ring format
        }

        foreach ($value as $ring) {
            if (!is_array($ring) || empty($ring)) {
                throw new Exception('Each ring in Polygon must be an array of points');
            }

            foreach ($ring as $point) {
                if (!is_array($point) || count($point) !== 2) {
                    throw new Exception('Each point in Polygon ring must be an array of two values [x, y]');
                }
                if (!is_numeric($point[0]) || !is_numeric($point[1])) {
                    throw new Exception('Each point in Polygon ring must have numeric coordinates');
                }
            }
        }

        return true;
    }

    /**
     * Check if a value is valid WKT string
     *
     * @param string $value
     * @return bool
     */
    public static function isWKTString(string $value): bool
    {
        $value = trim($value);
        return (bool) preg_match('/^(POINT|LINESTRING|POLYGON)\s*\(/i', $value);
    }

    /**
     * Get validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return 'Value must be a valid ' . $this->spatialType . ' format (array or WKT string)';
    }

    /**
     * Is array
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * @return string
     */
    public function getType(): string
    {
        return 'spatial';
    }

    /**
     * Is valid
     *
     * @param mixed $value
     * @return bool
     */
    public function isValid($value): bool
    {
        if (is_null($value)) {
            return true;
        }

        if (is_string($value)) {
            // Check if it's a valid WKT string
            return self::isWKTString($value);
        }

        if (is_array($value)) {
            // Validate the array format according to the specific spatial type
            try {
                self::validate($value, $this->spatialType);
                return true;
            } catch (\Exception $e) {
                return false;
            }
        }

        return false;
    }
}
