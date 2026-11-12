<?php

namespace Tests\Unit;

use Utopia\Database\Document;
use Utopia\Database\SetType;

final class SetRecordingDocument extends Document
{
    /**
     * @var list<string>
     */
    public array $sets = [];

    #[\Override]
    public function setAttribute(string $key, mixed $value, SetType $type = SetType::Assign): static
    {
        $this->sets[] = $key;

        return parent::setAttribute($key, $value, $type);
    }
}
