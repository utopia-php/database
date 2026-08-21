<?php

namespace Utopia\Database\Migration;

use Utopia\Database\Schema\Change;
use Utopia\Database\Schema\ChangeType;
use Utopia\Database\Schema\DiffResult;

class Generator
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
        $downBody = $downLines !== [] ? \implode("\n", \array_reverse($downLines)) : '        // No changes';

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

    private function generateUpStatement(Change $change): ?string
    {
        $collectionId = $this->collectionId($change);

        return match ($change->type) {
            ChangeType::AddAttribute => $change->attribute !== null
                ? "\$db->createAttribute('{$collectionId}', new \\Utopia\\Database\\Attribute(key: '{$change->attribute->key}', type: \\Utopia\\Query\\Schema\\ColumnType::" . \ucfirst($change->attribute->type->value) . ", size: {$change->attribute->size}));"
                : null,
            ChangeType::DropAttribute => $change->attribute !== null
                ? "\$db->deleteAttribute('{$collectionId}', '{$change->attribute->key}');"
                : null,
            ChangeType::AddIndex => $change->index !== null
                ? "\$db->createIndex('{$collectionId}', new \\Utopia\\Database\\Index(key: '{$change->index->key}', type: \\Utopia\\Query\\Schema\\IndexType::" . \ucfirst($change->index->type->value) . ", attributes: " . \var_export($change->index->attributes, true) . '));'
                : null,
            ChangeType::DropIndex => $change->index !== null
                ? "\$db->deleteIndex('{$collectionId}', '{$change->index->key}');"
                : null,
            default => null,
        };
    }

    private function generateDownStatement(Change $change): ?string
    {
        $collectionId = $this->collectionId($change);

        return match ($change->type) {
            ChangeType::AddAttribute => $change->attribute !== null
                ? "\$db->deleteAttribute('{$collectionId}', '{$change->attribute->key}');"
                : null,
            ChangeType::DropAttribute => $change->attribute !== null
                ? "\$db->createAttribute('{$collectionId}', new \\Utopia\\Database\\Attribute(key: '{$change->attribute->key}', type: \\Utopia\\Query\\Schema\\ColumnType::" . \ucfirst($change->attribute->type->value) . ", size: {$change->attribute->size}));"
                : null,
            ChangeType::AddIndex => $change->index !== null
                ? "\$db->deleteIndex('{$collectionId}', '{$change->index->key}');"
                : null,
            ChangeType::DropIndex => $change->index !== null
                ? "\$db->createIndex('{$collectionId}', new \\Utopia\\Database\\Index(key: '{$change->index->key}', type: \\Utopia\\Query\\Schema\\IndexType::" . \ucfirst($change->index->type->value) . ", attributes: " . \var_export($change->index->attributes, true) . '));'
                : null,
            default => null,
        };
    }

    private function collectionId(Change $change): string
    {
        if ($change->collectionId === null || $change->collectionId === '') {
            return '{collectionId}';
        }

        return $change->collectionId;
    }
}
