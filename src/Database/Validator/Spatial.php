<?php

namespace Utopia\Database\Validator;

use Utopia\Query\Schema\ColumnType;
use Utopia\Validator;

class Spatial extends Validator
{
    private string $spatialType;

    protected string $message = '';

    /**
     * @param string $spatialType The spatial type to validate (point, linestring, polygon)
     */
    public function __construct(string $spatialType)
    {
        $this->spatialType = $spatialType;
    }

    /**
     * Validate POINT data
     *
     * @param  array<mixed>  $value
     */
    protected function validatePoint(array $value): bool
    {
        if (count($value) !== 2 || ! array_is_list($value)) {
            $this->message = 'Point must be an array of two numeric values [x, y]';

            return false;
        }

        if (! is_numeric($value[0]) || ! is_numeric($value[1])) {
            $this->message = 'Point coordinates must be numeric values';

            return false;
        }

        return $this->isValidCoordinate((float) $value[0], (float) $value[1]);
    }

    /**
     * Validate LINESTRING data
     *
     * @param  array<mixed>  $value
     */
    protected function validateLineString(array $value): bool
    {
        if (count($value) < 2) {
            $this->message = 'LineString must contain at least two points';

            return false;
        }

        foreach ($value as $pointIndex => $point) {
            if (! is_array($point) || count($point) !== 2 || ! array_is_list($point)) {
                $this->message = 'Each point in LineString must be an array of two values [x, y]';

                return false;
            }

            if (! is_numeric($point[0]) || ! is_numeric($point[1])) {
                $this->message = 'Each point in LineString must have numeric coordinates';

                return false;
            }

            if (! $this->isValidCoordinate((float) $point[0], (float) $point[1])) {
                $this->message = "Invalid coordinates at point #{$pointIndex}: {$this->message}";

                return false;
            }
        }

        return true;
    }

    /**
     * Validate POLYGON data
     *
     * @param  array<mixed>  $value
     */
    protected function validatePolygon(array $value): bool
    {
        if (empty($value)) {
            $this->message = 'Polygon must contain at least one ring';

            return false;
        }

        $isSingleRing = isset($value[0]) && is_array($value[0]) &&
            count($value[0]) === 2 && array_is_list($value[0]) &&
            is_numeric($value[0][0]) &&
            is_numeric($value[0][1]);

        if ($isSingleRing) {
            $value = [$value];
        }

        foreach ($value as $ringIndex => $ring) {
            if (! is_array($ring) || empty($ring) || ! array_is_list($ring)) {
                $this->message = "Ring #{$ringIndex} must be an array of points";

                return false;
            }

            if (count($ring) < 4) {
                $this->message = "Ring #{$ringIndex} must contain at least 4 points to form a closed polygon";

                return false;
            }

            foreach ($ring as $pointIndex => $point) {
                if (! is_array($point) || count($point) !== 2 || ! array_is_list($point)) {
                    $this->message = "Point #{$pointIndex} in ring #{$ringIndex} must be an array of two values [x, y]";

                    return false;
                }

                if (! is_numeric($point[0]) || ! is_numeric($point[1])) {
                    $this->message = "Coordinates of point #{$pointIndex} in ring #{$ringIndex} must be numeric";

                    return false;
                }

                if (! $this->isValidCoordinate((float) $point[0], (float) $point[1])) {
                    $this->message = "Invalid coordinates at point #{$pointIndex} in ring #{$ringIndex}: {$this->message}";

                    return false;
                }
            }

            if ($ring[0] !== $ring[count($ring) - 1]) {
                $this->message = "Ring #{$ringIndex} must be closed (first point must equal last point)";

                return false;
            }
        }

        return true;
    }

    /**
     * Whether a value is well-formed WKT (Well-Known Text) of a point, linestring or polygon. Its coordinates are not
     * checked; isValid() checks a stored value's.
     */
    public static function isWktString(string $value): bool
    {
        return self::parseWkt($value) !== null;
    }

    /**
     * The type and coordinates WKT names, as a stored value of that type holds them: `[x, y]` for a point, a list of
     * points for a linestring and a list of rings for a polygon. Null for anything that is not well-formed WKT of a
     * point, linestring or polygon.
     *
     * @return array{ColumnType, array<mixed>}|null
     */
    public static function parseWkt(string $value): ?array
    {
        $value = \trim($value);
        $open = \strpos($value, '(');
        if ($open === false || ! \str_ends_with($value, ')')) {
            return null;
        }

        $type = match (\strtoupper(\trim(\substr($value, 0, $open)))) {
            'POINT' => ColumnType::Point,
            'LINESTRING' => ColumnType::Linestring,
            'POLYGON' => ColumnType::Polygon,
            default => null,
        };
        if ($type === null) {
            return null;
        }

        $body = \substr($value, $open + 1, -1);
        $coordinates = match ($type) {
            ColumnType::Point => self::parseWktPoint($body),
            ColumnType::Linestring => self::parseWktPoints($body),
            default => self::parseWktRings($body),
        };

        return $coordinates === null ? null : [$type, $coordinates];
    }

    /**
     * @return list<float>|null
     */
    private static function parseWktPoint(string $text): ?array
    {
        $tokens = \array_values(\array_filter(
            \explode(' ', \str_replace(["\t", "\n", "\r"], ' ', $text)),
            static fn (string $token): bool => $token !== '',
        ));

        if (\count($tokens) !== 2 || ! \is_numeric($tokens[0]) || ! \is_numeric($tokens[1])) {
            return null;
        }

        return [(float) $tokens[0], (float) $tokens[1]];
    }

    /**
     * @return list<list<float>>|null
     */
    private static function parseWktPoints(string $text): ?array
    {
        $points = [];
        foreach (\explode(',', $text) as $point) {
            $parsed = self::parseWktPoint($point);
            if ($parsed === null) {
                return null;
            }
            $points[] = $parsed;
        }

        return $points;
    }

    /**
     * @return list<list<list<float>>>|null
     */
    private static function parseWktRings(string $text): ?array
    {
        $rings = [];
        $rest = \trim($text);
        while ($rest !== '') {
            if (! \str_starts_with($rest, '(')) {
                return null;
            }

            $close = \strpos($rest, ')');
            if ($close === false) {
                return null;
            }

            $ring = self::parseWktPoints(\substr($rest, 1, $close - 1));
            if ($ring === null) {
                return null;
            }
            $rings[] = $ring;

            $rest = \trim(\substr($rest, $close + 1));
            if ($rest === '') {
                break;
            }
            if (! \str_starts_with($rest, ',')) {
                return null;
            }
            $rest = \trim(\substr($rest, 1));
            if ($rest === '') {
                return null;
            }
        }

        return $rings === [] ? null : $rings;
    }

    #[\Override]
    public function getDescription(): string
    {
        return 'Value must be a valid '.$this->spatialType.": {$this->message}";
    }

    #[\Override]
    public function isArray(): bool
    {
        return false;
    }

    #[\Override]
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }

    public function getSpatialType(): string
    {
        return $this->spatialType;
    }

    /**
     * Validate a spatial value as an array of coordinates or a WKT string.
     *
     * @param mixed $value The spatial data to validate
     * @return bool
     */
    #[\Override]
    public function isValid(mixed $value): bool
    {
        if (is_null($value)) {
            return true;
        }

        if (is_string($value)) {
            $wkt = self::parseWkt($value);
            if ($wkt === null) {
                $this->message = 'WKT must be a well-formed POINT, LINESTRING or POLYGON';

                return false;
            }

            [$type, $value] = $wkt;
            if ($type->value !== $this->spatialType) {
                $this->message = 'WKT must be a '.\strtoupper($this->spatialType).', got a '.\strtoupper($type->value);

                return false;
            }
        }

        if (is_array($value)) {
            $spatialColumnType = ColumnType::tryFrom($this->spatialType);
            switch ($spatialColumnType) {
                case ColumnType::Point:
                    return $this->validatePoint($value);

                case ColumnType::Linestring:
                    return $this->validateLineString($value);

                case ColumnType::Polygon:
                    return $this->validatePolygon($value);

                default:
                    $this->message = 'Unknown spatial type: '.$this->spatialType;

                    return false;
            }
        }

        $this->message = 'Spatial value must be array or WKT string';

        return false;
    }

    private function isValidCoordinate(int|float $x, int|float $y): bool
    {
        if (! \is_finite($x) || ! \is_finite($y)) {
            $this->message = 'Coordinates must be finite numbers';

            return false;
        }

        if ($x < -180 || $x > 180) {
            $this->message = "Longitude (x) must be between -180 and 180, got {$x}";

            return false;
        }

        if ($y < -90 || $y > 90) {
            $this->message = "Latitude (y) must be between -90 and 90, got {$y}";

            return false;
        }

        return true;
    }
}
