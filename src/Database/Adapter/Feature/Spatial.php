<?php

namespace Utopia\Database\Adapter\Feature;

interface Spatial
{
    /**
     * @return array<float>
     */
    public function decodePoint(string $wkb): array;

    /**
     * @return array<array<float>>
     */
    public function decodeLinestring(string $wkb): array;

    /**
     * @return array<array<array<float>>>
     */
    public function decodePolygon(string $wkb): array;
}
