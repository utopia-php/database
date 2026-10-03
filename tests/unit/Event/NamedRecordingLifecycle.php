<?php

namespace Tests\Unit\Event;

use Utopia\Database\Hook\Named;

final class NamedRecordingLifecycle extends RecordingLifecycle implements Named
{
    public function __construct(
        private readonly string $name,
    ) {
    }

    public function getName(): string
    {
        return $this->name;
    }
}
