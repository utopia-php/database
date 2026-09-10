<?php

namespace Utopia\Database\Type;

use Utopia\Database\Attribute;

interface Embeddable
{
    public function name(): string;

    /**
     * @return array<Attribute>
     */
    public function attributes(): array;

    /**
     * @return array<string, mixed>
     */
    public function decompose(mixed $value): array;

    /**
     * @param  array<string, mixed>  $values
     */
    public function compose(array $values): mixed;
}
