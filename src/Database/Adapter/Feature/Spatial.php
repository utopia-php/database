<?php

namespace Utopia\Database\Adapter\Feature;

/**
 * Defines spatial geometry decoding operations for a database adapter.
 */
interface Spatial
{
    /**
     * Decode a WKB-encoded point into coordinates.
     *
     * @param string $wkb The Well-Known Binary representation.
     * @return array<float> The point as [longitude, latitude].
     */
    public function decodePoint(string $wkb): array;

    /**
     * Decode a WKB-encoded linestring into an array of coordinate pairs.
     *
     * @param string $wkb The Well-Known Binary representation.
     * @return array<array<float>> Array of [longitude, latitude] pairs.
     */
    public function decodeLinestring(string $wkb): array;

    /**
     * Decode a WKB-encoded polygon into an array of rings, each containing coordinate pairs.
     *
     * @param string $wkb The Well-Known Binary representation.
     * @return array<array<array<float>>> Array of rings, each an array of [longitude, latitude] pairs.
     */
    public function decodePolygon(string $wkb): array;
}
