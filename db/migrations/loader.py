import pkgutil
import sys
from importlib import import_module, reload

from django.apps import apps
from django.conf import settings
from django.db.migrations.graph import MigrationGraph
from django.db.migrations.recorder import MigrationRecorder

from .exceptions import (
    AmbiguityError,
    BadMigrationError,
    InconsistentMigrationHistory,
    NodeNotFoundError,
)

MIGRATIONS_MODULE_NAME = "migrations"


# 加载生成的 migrations 迁移文件
class MigrationLoader:
    """
    Load migration files from disk and their status from the database.

    Migration files are expected to live in the "migrations" directory of
    an app. Their names are entirely unimportant from a code perspective,
    but will probably follow the 1234_name.py convention.

    On initialization, this class will scan those directories, and open and
    read the Python files, looking for a class called Migration, which should
    inherit from django.db.migrations.Migration. See
    django.db.migrations.migration for what that looks like.

    Some migrations will be marked as "replacing" another set of migrations.
    These are loaded into a separate set of migrations away from the main ones.
    If all the migrations they replace are either unapplied or missing from
    disk, then they are injected into the main set, replacing the named migrations.
    Any dependency pointers to the replaced migrations are re-pointed to the
    new migration.

    This does mean that this class MUST also talk to the database as well as
    to disk, but this is probably fine. We're already not just operating
    in memory.
    """

    # 初始化需要传递一个 connection，
    def __init__(
        self,
        connection,
        load=True,
        ignore_no_migrations=False,
        replace_migrations=True,
    ):
        self.connection = connection
        self.disk_migrations = None
        self.applied_migrations = None
        self.ignore_no_migrations = ignore_no_migrations
        self.replace_migrations = replace_migrations
        # 构建迁移图
        if load:
            self.build_graph()

    @classmethod
    def migrations_module(cls, app_label):
        """
        返回指定 app_label 的迁移模块的路径
        """
        if app_label in settings.MIGRATION_MODULES:
            return settings.MIGRATION_MODULES[app_label], True
        else:
            app_package_name = apps.get_app_config(app_label).name
            return "%s.%s" % (app_package_name, MIGRATIONS_MODULE_NAME), False

    def load_disk(self):
        """Load the migrations from all INSTALLED_APPS from disk."""
        self.disk_migrations = {}     # 加载的磁盘上的所有应用的 migration 的映射，形如：{(auth, 0001_initial): Migration(0001_initial, auth)}，代表的是 auth 应用下的 0001_initial 这个 migration
        self.unmigrated_apps = set()  # 未迁移的应用
        self.migrated_apps = set()    # 已迁移的应用

        #
        for app_config in apps.get_app_configs():
            # app_config.label 获取到的是 setting 里面的应用的名称
            # 获取应用 migrations 模块的路径。如果自己创建了一个叫做 books 的应用，那么 module_name 的值就是 books.migrations
            module_name, explicit = self.migrations_module(app_config.label)
            if module_name is None:
                self.unmigrated_apps.add(app_config.label)
                continue
            was_loaded = module_name in sys.modules
            # 导入项目的 migrations 模块
            try:
                module = import_module(module_name)
            except ModuleNotFoundError as e:
                if (explicit and self.ignore_no_migrations) or (
                    not explicit and MIGRATIONS_MODULE_NAME in e.name.split(".")
                ):
                    self.unmigrated_apps.add(app_config.label)
                    continue
                raise
            else:
                # Module is not a package (e.g. migrations.py).
                if not hasattr(module, "__path__"):
                    self.unmigrated_apps.add(app_config.label)
                    continue
                # Empty directories are namespaces. Namespace packages have no
                # __file__ and don't use a list for __path__. See
                # https://docs.python.org/3/reference/import.html#namespace-packages
                if getattr(module, "__file__", None) is None and not isinstance(
                    module.__path__, list
                ):
                    self.unmigrated_apps.add(app_config.label)
                    continue
                # 如果已经加载，则强制重新加载
                # Force a reload if it's already loaded (tests need this)
                if was_loaded:
                    reload(module)

            # 将次应用变更为已迁移的应用
            self.migrated_apps.add(app_config.label)
            # 获取到应用迁移模块下的所有迁移文件，例如内置 auth 应用下的 migrations 文件夹下的所有迁移文件，其实就是 django/contrib/auth/migrations 下的所有文件
            # migration_names 结果的值就是 {0001_initial, 0002_alter_permission_name_max_length, ......}
            migration_names = {
                name
                for _, name, is_pkg in pkgutil.iter_modules(module.__path__)
                if not is_pkg and name[0] not in "_~"
            }
            # 加载每个 migration 文件
            # Load migrations
            for migration_name in migration_names:
                # 拼接得到完整的 migration 文件的路径
                migration_path = "%s.%s" % (module_name, migration_name)
                try:
                    # 加载 migration 文件
                    migration_module = import_module(migration_path)
                except ImportError as e:
                    if "bad magic number" in str(e):
                        raise ImportError(
                            "Couldn't import %r as it appears to be a stale "
                            ".pyc file." % migration_path
                        ) from e
                    else:
                        raise
                # 迁移文件中必须包含 Migration 类，否则抛出异常
                if not hasattr(migration_module, "Migration"):
                    raise BadMigrationError(
                        "Migration %s in app %s has no Migration class"
                        % (migration_name, app_config.label)
                    )
                # disk_migrations 是一个字典，key 是一个二元组，(应用名称，migration 名称)，例如(auth, 0001_initial)
                # disk_migrations 的值是对应的 migration文件的 Migration 类的实例对象，也就是实例话每个 migration 文件中的 Migration 类。例如 Migration(0001_initial, auth)
                # 项目中生成的每个 migration 文件中的每个 Migration 类都是继承 django/db/migrations/migration.py 中的 Migration 的，他的初始化需要两个参数，分别为 name, app_label。name 代表 migration 的名称，app_label 则代表应用的名称。
                self.disk_migrations[
                    app_config.label, migration_name
                ] = migration_module.Migration(
                    migration_name,
                    app_config.label,
                )

    def get_migration(self, app_label, name_prefix):
        """Return the named migration or raise NodeNotFoundError."""
        return self.graph.nodes[app_label, name_prefix]

    def get_migration_by_prefix(self, app_label, name_prefix):
        """
        Return the migration(s) which match the given app label and name_prefix.
        """
        # Do the search
        results = []
        for migration_app_label, migration_name in self.disk_migrations:
            if migration_app_label == app_label and migration_name.startswith(
                name_prefix
            ):
                results.append((migration_app_label, migration_name))
        if len(results) > 1:
            raise AmbiguityError(
                "There is more than one migration for '%s' with the prefix '%s'"
                % (app_label, name_prefix)
            )
        elif not results:
            raise KeyError(
                f"There is no migration for '{app_label}' with the prefix "
                f"'{name_prefix}'"
            )
        else:
            return self.disk_migrations[results[0]]

    def check_key(self, key, current_app):
        if (key[1] != "__first__" and key[1] != "__latest__") or key in self.graph:
            return key
        # Special-case __first__, which means "the first migration" for
        # migrated apps, and is ignored for unmigrated apps. It allows
        # makemigrations to declare dependencies on apps before they even have
        # migrations.
        if key[0] == current_app:
            # Ignore __first__ references to the same app (#22325)
            return
        if key[0] in self.unmigrated_apps:
            # This app isn't migrated, but something depends on it.
            # The models will get auto-added into the state, though
            # so we're fine.
            return
        if key[0] in self.migrated_apps:
            try:
                if key[1] == "__first__":
                    return self.graph.root_nodes(key[0])[0]
                else:  # "__latest__"
                    return self.graph.leaf_nodes(key[0])[0]
            except IndexError:
                if self.ignore_no_migrations:
                    return None
                else:
                    raise ValueError(
                        "Dependency on app with no migrations: %s" % key[0]
                    )
        raise ValueError("Dependency on unknown app: %s" % key[0])

    # 添加同一个应用里面的依赖的关系
    """
    假如一个 migration 文件的 dependencies 信息如下：
    dependencies = [
        ("auth", "0002_alter_permission_name_max_length"),
    ]
    key 相当于 auth，migration 相当于 0002_alter_permission_name_max_length

    还有一种 dependencies 如下：
    dependencies = [
        ("contenttypes", "__first__"),
    ]
    这个就代表依赖 contenttypes 里面的 __first__，即 django/contrib/contenttypes/migrations/0001_initial.py 中的 Migration
    当然下面方法中的 self.graph.add_dependency 排除了 __first__ 的这种 migration
    """
    def add_internal_dependencies(self, key, migration):
        """
        Internal dependencies need to be added first to ensure `__first__`
        dependencies find the correct root node.
        """
        for parent in migration.dependencies:
            # parent[0] == key[0] 代表的是同一个应用，parent[1] != "__first__" 代表不是第一个 migration 文件
            if parent[0] == key[0] and parent[1] != "__first__":
                self.graph.add_dependency(migration, key, parent, skip_validation=True)

    # 添加外部依赖
    def add_external_dependencies(self, key, migration):
        for parent in migration.dependencies:
            # 跳过内部依赖
            if key[0] == parent[0]:
                continue
            parent = self.check_key(parent, key[0])
            if parent is not None:
                self.graph.add_dependency(migration, key, parent, skip_validation=True)
        # 处理 run_before。如果有 run_before，需要遍历加入 graph 中
        for child in migration.run_before:
            child = self.check_key(child, key[0])
            if child is not None:
                self.graph.add_dependency(migration, child, key, skip_validation=True)

    def build_graph(self):
        """
        Build a migration dependency graph using both the disk and database.
        You'll need to rebuild the graph if you apply migrations. This isn't
        usually a problem as generally migration stuff runs in a one-shot process.
        """
        # 从磁盘上加载每个应用的 migration 文件
        # Load disk data
        self.load_disk()

        # 没有 connection 那么将 applied_migrations 设置空
        # Load database data
        if self.connection is None:
            self.applied_migrations = {}
        else:
            # 创建MigrationRecorder 对象，调用 applied_migrations 方法查询已经存在数据库 django_migrations 表中的 migration 的执行记录
            recorder = MigrationRecorder(self.connection)
            self.applied_migrations = recorder.applied_migrations()
        # To start, populate the migration graph with nodes for ALL migrations
        # and their dependencies. Also make note of replacing migrations at this step.
        # 创建一个迁移图
        self.graph = MigrationGraph()
        self.replacements = {}

        # 第一次遍历，遍历从磁盘加载的 migration 信息，创建 node，并且记录替换信息
        for key, migration in self.disk_migrations.items():
            self.graph.add_node(key, migration)
            # 如果该 migration 有 replaces，那么记录到 replacements 中
            if migration.replaces:
                self.replacements[key] = migration

        # 第二次遍历，遍历从磁盘加载的 migration 信息，添加内部依赖
        for key, migration in self.disk_migrations.items():
            # Internal (same app) dependencies.
            self.add_internal_dependencies(key, migration)

        # 第三次遍历，遍历从磁盘加载的 migration 信息，添加外部依赖
        for key, migration in self.disk_migrations.items():
            self.add_external_dependencies(key, migration)

        # 替换 migration
        if self.replace_migrations:
            for key, migration in self.replacements.items():
                # Get applied status of each of this migration's replacement
                # targets.
                applied_statuses = [
                    (target in self.applied_migrations) for target in migration.replaces
                ]
                # The replacing migration is only marked as applied if all of
                # its replacement targets are.
                if all(applied_statuses):
                    self.applied_migrations[key] = migration
                else:
                    self.applied_migrations.pop(key, None)
                # A replacing migration can be used if either all or none of
                # its replacement targets have been applied.
                if all(applied_statuses) or (not any(applied_statuses)):
                    self.graph.remove_replaced_nodes(key, migration.replaces)
                else:
                    # This replacing migration cannot be used because it is
                    # partially applied. Remove it from the graph and remap
                    # dependencies to it (#25945).
                    self.graph.remove_replacement_node(key, migration.replaces)

        # 判断是否存在虚拟节点
        try:
            self.graph.validate_consistency()
        except NodeNotFoundError as exc:
            # Check if the missing node could have been replaced by any squash
            # migration but wasn't because the squash migration was partially
            # applied before. In that case raise a more understandable exception
            # (#23556).
            # Get reverse replacements.
            reverse_replacements = {}
            for key, migration in self.replacements.items():
                for replaced in migration.replaces:
                    reverse_replacements.setdefault(replaced, set()).add(key)
            # Try to reraise exception with more detail.
            if exc.node in reverse_replacements:
                candidates = reverse_replacements.get(exc.node, set())
                is_replaced = any(
                    candidate in self.graph.nodes for candidate in candidates
                )
                if not is_replaced:
                    tries = ", ".join("%s.%s" % c for c in candidates)
                    raise NodeNotFoundError(
                        "Migration {0} depends on nonexistent node ('{1}', '{2}'). "
                        "Django tried to replace migration {1}.{2} with any of [{3}] "
                        "but wasn't able to because some of the replaced migrations "
                        "are already applied.".format(
                            exc.origin, exc.node[0], exc.node[1], tries
                        ),
                        exc.node,
                    ) from exc
            raise

        # 判断 graph 是否存在循环
        self.graph.ensure_not_cyclic()

    # 检查迁移记录，用于检查数据库中的 migration 记录是否正常。
    def check_consistent_history(self, connection):
        """
        Raise InconsistentMigrationHistory if any applied migrations have
        unapplied dependencies.
        """
        # 实例话 MigrationRecorder
        recorder = MigrationRecorder(connection)
        # 查询数据库中的迁移记录
        applied = recorder.applied_migrations()
        for migration in applied:
            # 如果数据库表 django_migrations 中的迁移记录，没有在当前的 graph 中，那么就跳过
            if migration not in self.graph.nodes:
                continue

            # 检查数据库记录中的 migration 对象的 parents，所依赖的 parents 也是存在于数据库中的
            for parent in self.graph.node_map[migration].parents:
                if parent not in applied:
                    # Skip unapplied squashed migrations that have all of their
                    # `replaces` applied.
                    if parent in self.replacements:
                        if all(
                            m in applied for m in self.replacements[parent].replaces
                        ):
                            continue
                    raise InconsistentMigrationHistory(
                        "Migration {}.{} is applied before its dependency "
                        "{}.{} on database '{}'.".format(
                            migration[0],
                            migration[1],
                            parent[0],
                            parent[1],
                            connection.alias,
                        )
                    )

    # 检测是否冲突
    def detect_conflicts(self):
        """
        Look through the loaded graph and detect any conflicts - apps
        with more than one leaf migration. Return a dict of the app labels
        that conflict with the migration names that conflict.
        """
        seen_apps = {}
        conflicting_apps = set()
        # 获取所有的叶子节点的应用名称和 migration 名称
        for app_label, migration_name in self.graph.leaf_nodes():
            if app_label in seen_apps:
                conflicting_apps.add(app_label)
            seen_apps.setdefault(app_label, set()).add(migration_name)
        return {
            app_label: sorted(seen_apps[app_label]) for app_label in conflicting_apps
        }

    def project_state(self, nodes=None, at_end=True):
        """
        Return a ProjectState object representing the most recent state
        that the loaded migrations represent.

        See graph.make_state() for the meaning of "nodes" and "at_end".
        """
        return self.graph.make_state(
            nodes=nodes, at_end=at_end, real_apps=self.unmigrated_apps
        )

    def collect_sql(self, plan):
        """
        Take a migration plan and return a list of collected SQL statements
        that represent the best-efforts version of that plan.
        """
        statements = []
        state = None
        for migration, backwards in plan:
            with self.connection.schema_editor(
                collect_sql=True, atomic=migration.atomic
            ) as schema_editor:
                if state is None:
                    state = self.project_state(
                        (migration.app_label, migration.name), at_end=False
                    )
                if not backwards:
                    state = migration.apply(state, schema_editor, collect_sql=True)
                else:
                    state = migration.unapply(state, schema_editor, collect_sql=True)
            statements.extend(schema_editor.collected_sql)
        return statements
