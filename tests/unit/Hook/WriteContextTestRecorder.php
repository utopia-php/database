<?php

namespace Tests\Unit\Hook;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Interceptor;
use Utopia\Database\Hook\WriteContext;
use Utopia\Database\Storage;

/**
 * Records what {@see WriteContextTest}'s writes hand their write hooks through the context.
 */
final class WriteContextTestRecorder extends Interceptor
{
    /** @var list<bool> */
    public array $skipPermissions = [];

    /** @var list<array{string, string}> */
    public array $updates = [];

    /** @var array<string, list<array{string, string, ?int}>> */
    public array $permissionRows = [];

    /** @var list<array<string, mixed>> */
    public array $decorated = [];

    #[\Override]
    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
    {
        foreach ($documents as $document) {
            $this->decorated[] = $context->decorateRow([], $document);
        }

        $table = Storage::permissionsTable($collection);
        $scoped = $context->builder($table)->sortAsc(Storage::PERMISSIONS_DOCUMENT)->build();
        $raw = $context->rawBuilder()->from($context->rawTable($table))->sortAsc(Storage::PERMISSIONS_DOCUMENT)->build();

        $this->permissionRows = [
            'scoped' => $this->rows($context->fetch($scoped, Event::PermissionsRead)),
            'raw' => $this->rows($context->fetch($raw, Event::PermissionsRead)),
        ];
    }

    #[\Override]
    public function afterDocumentUpdate(string $collection, string $id, Document $document, WriteContext $context): void
    {
        $this->skipPermissions[] = $context->skipPermissions($document);
        $this->updates[] = [$id, $document->getId()];
    }

    /**
     * @param  list<array<string, mixed>>  $rows
     * @return list<array{string, string, ?int}>
     */
    private function rows(array $rows): array
    {
        return \array_map(static function (array $row): array {
            $document = $row[Storage::PERMISSIONS_DOCUMENT] ?? null;
            $permission = $row[Storage::PERMISSIONS_PERMISSION] ?? null;
            $tenant = $row[Storage::TENANT] ?? null;

            return [
                \is_string($document) ? $document : '',
                \is_string($permission) ? $permission : '',
                \is_numeric($tenant) ? (int) $tenant : null,
            ];
        }, $rows);
    }
}
