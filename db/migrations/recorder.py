from django.apps.registry import Apps
from django.db import DatabaseError, models
from django.utils.functional import classproperty
from django.utils.timezone import now

from .exceptions import MigrationSchemaMissing


class MigrationRecorder:
    """
    迁移记录，对应数据库表中的 django_migrations 表
    """

    _migration_class = None

    @classproperty
    def Migration(cls):
        """
        懒加载 Migration
        """
        if cls._migration_class is None:

            class Migration(models.Model):
                app = models.CharField(max_length=255)
                name = models.CharField(max_length=255)
                applied = models.DateTimeField(default=now)

                class Meta:
                    apps = Apps()
                    app_label = "migrations"
                    db_table = "django_migrations"

                def __str__(self):
                    return "Migration %s for %s" % (self.name, self.app)

            cls._migration_class = Migration
        return cls._migration_class

    # 使用 MigrationRecorder 的时候需要传入一个 connection
    # 可以使用 django/db/__init__.py 里面的 connection，connection 返回 default 数据库的连接
    def __init__(self, connection):
        self.connection = connection

    # 查询所有记录
    @property
    def migration_qs(self):
        return self.Migration.objects.using(self.connection.alias)

    # 判断 django_migrations 表是否存在
    def has_table(self):
        """Return True if the django_migrations table exists."""
        with self.connection.cursor() as cursor:
            # 获取数据库所有表
            tables = self.connection.introspection.table_names(cursor)
        return self.Migration._meta.db_table in tables

    # 判断是否存在 django_migrations 表，不存在则创建
    def ensure_schema(self):
        """Ensure the table exists and has the correct schema."""
        # If the table's there, that's fine - we've never changed its schema
        # in the codebase.
        if self.has_table():
            return
        # Make the table
        try:
            with self.connection.schema_editor() as editor:
                editor.create_model(self.Migration)
        except DatabaseError as exc:
            raise MigrationSchemaMissing(
                "Unable to create the django_migrations table (%s)" % exc
            )

    # 返回所有的 migration 记录信息
    def applied_migrations(self):
        """
        Return a dict mapping (app_name, migration_name) to Migration instances
        for all applied migrations.
        """
        if self.has_table():
            return {
                (migration.app, migration.name): migration
                for migration in self.migration_qs
            }
        else:
            # If the django_migrations table doesn't exist, then no migrations
            # are applied.
            return {}

    # 新增一条 migration 记录信息
    def record_applied(self, app, name):
        """Record that a migration was applied."""
        self.ensure_schema()
        self.migration_qs.create(app=app, name=name)

    # 删除一条 migration 记录信息
    def record_unapplied(self, app, name):
        """Record that a migration was unapplied."""
        self.ensure_schema()
        self.migration_qs.filter(app=app, name=name).delete()

    # 删除所有 migration 记录信息
    def flush(self):
        """Delete all migration records. Useful for testing migrations."""
        self.migration_qs.all().delete()
