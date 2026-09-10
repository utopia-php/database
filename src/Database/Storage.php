<?php

namespace Utopia\Database;

final readonly class Storage
{
    public const string UID = '_uid';

    public const string SEQUENCE = '_id';

    public const string COLLECTION = '_collection';

    public const string TENANT = '_tenant';

    public const string CREATED_AT = '_createdAt';

    public const string UPDATED_AT = '_updatedAt';

    public const string PERMISSIONS = '_permissions';

    public const string VERSION = '_version';

    public const string DISTANCE = '_distance';

    public const string DELETED_AT = '_deletedAt';

    public const string PERMS_SUFFIX = '_perms';

    public const string PERM_DOCUMENT = '_document';

    public const string PERM_TYPE = '_type';

    public const string PERM_PERMISSION = '_permission';

    public const string INDEX_PRIMARY = 'primary';

    public const string INDEX_CREATED_AT = '_created_at';

    public const string INDEX_UPDATED_AT = '_updated_at';

    public const string INDEX_TENANT_ID = '_tenant_id';

    public const string INDEX_1 = '_index1';

    public const string INDEX_PERMISSIONS_ID = '_permissions_id';

    /**
     * @var array<string, string>
     */
    private const array ATTRIBUTE_MAP = [
        Document::ID => self::UID,
        Document::SEQUENCE => self::SEQUENCE,
        Document::COLLECTION => self::COLLECTION,
        Document::TENANT => self::TENANT,
        Document::CREATED_AT => self::CREATED_AT,
        Document::UPDATED_AT => self::UPDATED_AT,
        Document::DELETED_AT => self::DELETED_AT,
        Document::PERMISSIONS => self::PERMISSIONS,
        Document::VERSION => self::VERSION,
        Document::DISTANCE => self::DISTANCE,
    ];

    private function __construct()
    {
    }

    public static function column(string $attribute): string
    {
        return self::ATTRIBUTE_MAP[$attribute] ?? $attribute;
    }

    public static function attribute(string $column): string
    {
        return self::columnMap()[$column] ?? $column;
    }

    /**
     * @return array<string, string>
     */
    public static function attributeMap(): array
    {
        return self::ATTRIBUTE_MAP;
    }

    /**
     * @return array<string, string>
     */
    public static function columnMap(): array
    {
        return \array_flip(self::ATTRIBUTE_MAP);
    }

    public static function permissionsTable(string $collection): string
    {
        return $collection.self::PERMS_SUFFIX;
    }
}
