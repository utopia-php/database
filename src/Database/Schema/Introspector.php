<?php

namespace Utopia\Database\Schema;

use Utopia\Database\Collection;
use Utopia\Database\Database;

class Introspector
{
    public function __construct(
        private Database $db,
    ) {
    }

    public function introspectCollection(string $collectionId): Collection
    {
        $collectionDoc = $this->db->getCollection($collectionId);

        if ($collectionDoc->isEmpty()) {
            throw new \RuntimeException("Collection '{$collectionId}' not found");
        }

        return Collection::fromDocument($collectionDoc);
    }

    /**
     * @return array<Collection>
     */
    public function introspectDatabase(): array
    {
        $collections = $this->db->listCollections();
        $result = [];

        foreach ($collections as $doc) {
            $result[] = Collection::fromDocument($doc);
        }

        return $result;
    }

    public function generateEntityClass(string $collectionId, string $namespace = 'App\\Entity'): string
    {
        $collection = $this->introspectCollection($collectionId);
        $className = $this->toPascalCase($collection->name ?: $collection->id);

        $lines = [];
        $lines[] = '<?php';
        $lines[] = '';
        $lines[] = "namespace {$namespace};";
        $lines[] = '';
        $lines[] = 'use Utopia\Database\ORM\Mapping\Column;';
        $lines[] = 'use Utopia\Database\ORM\Mapping\Entity;';
        $lines[] = 'use Utopia\Database\ORM\Mapping\Id;';
        $lines[] = 'use Utopia\Database\ORM\Mapping\CreatedAt;';
        $lines[] = 'use Utopia\Database\ORM\Mapping\UpdatedAt;';
        $lines[] = 'use Utopia\Database\ORM\Mapping\Version;';
        $lines[] = 'use Utopia\Query\Schema\ColumnType;';
        $lines[] = '';
        $lines[] = "#[Entity(collection: '{$collection->id}')]";
        $lines[] = "class {$className}";
        $lines[] = '{';
        $lines[] = '    #[Id]';
        $lines[] = '    public string $id = \'\';';
        $lines[] = '';
        $lines[] = '    #[Version]';
        $lines[] = '    public ?int $version = null;';
        $lines[] = '';
        $lines[] = '    #[CreatedAt]';
        $lines[] = '    public ?string $createdAt = null;';
        $lines[] = '';
        $lines[] = '    #[UpdatedAt]';
        $lines[] = '    public ?string $updatedAt = null;';

        foreach ($collection->attributes as $attr) {
            $lines[] = '';
            $phpType = $this->columnTypeToPhpType($attr->type, $attr->required, $attr->array);
            $typeParam = $this->columnTypeToEnumString($attr->type);
            $sizeParam = $attr->size > 0 ? ", size: {$attr->size}" : '';
            $requiredParam = $attr->required ? ', required: true' : '';
            $defaultParam = '';

            if ($attr->default !== null) {
                $defaultParam = match (true) {
                    \is_string($attr->default) => " = '{$attr->default}'",
                    \is_bool($attr->default) => ' = ' . ($attr->default ? 'true' : 'false'),
                    \is_int($attr->default), \is_float($attr->default) => " = {$attr->default}",
                    default => '',
                };
            } elseif (! $attr->required) {
                $defaultParam = ' = null';
            }

            $lines[] = "    #[Column(type: {$typeParam}{$sizeParam}{$requiredParam})]";
            $lines[] = "    public {$phpType} \${$attr->key}{$defaultParam};";
        }

        $lines[] = '}';
        $lines[] = '';

        return \implode("\n", $lines);
    }

    private function toPascalCase(string $value): string
    {
        return \str_replace(' ', '', \ucwords(\str_replace(['_', '-'], ' ', $value)));
    }

    private function columnTypeToPhpType(\Utopia\Query\Schema\ColumnType $type, bool $required, bool $array): string
    {
        if ($array) {
            return 'array';
        }

        $base = match ($type) {
            \Utopia\Query\Schema\ColumnType::String,
            \Utopia\Query\Schema\ColumnType::Varchar,
            \Utopia\Query\Schema\ColumnType::Text,
            \Utopia\Query\Schema\ColumnType::MediumText,
            \Utopia\Query\Schema\ColumnType::LongText,
            \Utopia\Query\Schema\ColumnType::Enum,
            \Utopia\Query\Schema\ColumnType::Datetime,
            \Utopia\Query\Schema\ColumnType::Timestamp => 'string',
            \Utopia\Query\Schema\ColumnType::Integer,
            \Utopia\Query\Schema\ColumnType::BigInteger => 'int',
            \Utopia\Query\Schema\ColumnType::Float,
            \Utopia\Query\Schema\ColumnType::Double => 'float',
            \Utopia\Query\Schema\ColumnType::Boolean => 'bool',
            default => 'mixed',
        };

        return $required ? $base : "?{$base}";
    }

    private function columnTypeToEnumString(\Utopia\Query\Schema\ColumnType $type): string
    {
        return 'ColumnType::' . \ucfirst($type->value);
    }
}
