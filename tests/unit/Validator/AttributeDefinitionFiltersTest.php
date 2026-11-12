<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\Profiles;
use Utopia\Database\Attribute;
use Utopia\Database\Filter;
use Utopia\Database\Validator\AttributeDefinition;

final class AttributeDefinitionFiltersTest extends TestCase
{
    public function testADeclarationListingItsFiltersAsEnumCasesIsValid(): void
    {
        $validator = new AttributeDefinition(attributes: [], profile: Profiles::of());
        $declaration = Attribute::datetime(key: 'publishedAt')->toDocument()->setAttribute('filters', [Filter::Datetime]);

        $this->assertTrue($validator->isValid($declaration), $validator->getDescription());
    }
}
