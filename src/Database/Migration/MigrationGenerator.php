<?php

namespace Utopia\Database\Migration;

use Utopia\Database\Schema\DiffResult;
use Utopia\Database\Schema\SchemaChange;
use Utopia\Database\Schema\SchemaChangeType;

class MigrationGenerator
{
    public function generate(DiffResult $diff, string $className, string $namespace = 'App\\Migration'): string
    {
        $version = $this->extractVersion($className);
        $upLines = [];
        $downLines = [];

        foreach ($diff->changes as $change) {
            $up = $this->generateUpStatement($change);
            $down = $this->generateDownStatement($change);

            if ($up !== null) {
                $upLines[] = "        {$up}";
            }

            if ($down !== null) {
                $downLines[] = "        {$down}";
            }
        }

        $upBody = $upLines !== [] ? \implode("\n", $upLines) : '        // No changes';
        $downBody = $downLines !== [] ? \implode("\n", $downLines) : '        // No changes';

        return <<<PHP
        <?php

        namespace {$namespace};

        use Utopia\Database\Database;
        use Utopia\Database\Migration\Migration;

        class {$className} extends Migration
        {
            public function version(): string
            {
                return '{$version}';
            }

            public function up(Database \$db): void
            {
        {$upBody}
            }

            public function down(Database \$db): void
            {
        {$downBody}
            }
        }

        PHP;
    }

    public function generateEmpty(string $className, string $namespace = 'App\\Migration'): string
    {
        $version = $this->extractVersion($className);

        return <<<PHP
        <?php

        namespace {$namespace};

        use Utopia\Database\Database;
        use Utopia\Database\Migration\Migration;

        class {$className} extends Migration
        {
            public function version(): string
            {
                return '{$version}';
            }

            public function up(Database \$db): void
            {
                //
            }

            public function down(Database \$db): void
            {
                //
            }
        }

        PHP;
    }

    private function extractVersion(string $className): string
    {
        if (\preg_match('/^V(\d+)_/', $className, $matches)) {
            return $matches[1];
        }

        return $className;
    }

    private function generateUpStatement(SchemaChange $change): ?string
    {
        return match ($change->type) {
            SchemaChangeType::AddAttribute => $change->attribute !== null
                ? "\$db->createAttribute('{collectionId}', new \\Utopia\\Database\\Attribute(key: '{$change->attribute->key}', type: \\Utopia\\Query\\Schema\\ColumnType::" . \ucfirst($change->attribute->type->value) . ", size: {$change->attribute->size}));"
                : null,
            SchemaChangeType::DropAttribute => $change->attribute !== null
                ? "\$db->deleteAttribute('{collectionId}', '{$change->attribute->key}');"
                : null,
            SchemaChangeType::AddIndex => $change->index !== null
                ? "\$db->createIndex('{collectionId}', new \\Utopia\\Database\\Index(key: '{$change->index->key}', type: \\Utopia\\Query\\Schema\\IndexType::" . \ucfirst($change->index->type->value) . ", attributes: " . \var_export($change->index->attributes, true) . '));\\'
                : null,
            SchemaChangeType::DropIndex => $change->index !== null
                ? "\$db->deleteIndex('{collectionId}', '{$change->index->key}');"
                : null,
            default => null,
        };
    }

    private function generateDownStatement(SchemaChange $change): ?string
    {
        return match ($change->type) {
            SchemaChangeType::AddAttribute => $change->attribute !== null
                ? "\$db->deleteAttribute('{collectionId}', '{$change->attribute->key}');"
                : null,
            SchemaChangeType::DropAttribute => $change->attribute !== null
                ? "\$db->createAttribute('{collectionId}', new \\Utopia\\Database\\Attribute(key: '{$change->attribute->key}', type: \\Utopia\\Query\\Schema\\ColumnType::" . \ucfirst($change->attribute->type->value) . ", size: {$change->attribute->size}));"
                : null,
            SchemaChangeType::AddIndex => $change->index !== null
                ? "\$db->deleteIndex('{collectionId}', '{$change->index->key}');"
                : null,
            SchemaChangeType::DropIndex => $change->index !== null
                ? "\$db->createIndex('{collectionId}', new \\Utopia\\Database\\Index(key: '{$change->index->key}', type: \\Utopia\\Query\\Schema\\IndexType::" . \ucfirst($change->index->type->value) . ", attributes: " . \var_export($change->index->attributes, true) . '));\\'
                : null,
            default => null,
        };
    }
}
