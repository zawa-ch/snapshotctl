#! /bin/bash
: << __LICENSE__
	MIT License

	Copyright (c) 2023 zawa-ch.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
__LICENSE__

# BACKUP_BIN_LOCATION="$(cd "$(dirname "$0")" && pwd)" || exit
# readonly BACKUP_BIN_LOCATION
readonly BACKUP_DB_PREFIX='backup-'
readonly BACKUP_DB_SCHEMA_REVISION=1

#	---- 設定項目 ----
#	スクリプトの動作を変更する環境変数とそのデフォルトの値
#	基本的にはここを変更するの**ではなく**、環境変数を設定してこのスクリプトを実行することを推奨する

#	スナップショット管理ディレクトリのルート
#	スナップショットを管理するためのデータベース等を配置する基点となるディレクトリ
#	設定を省略した場合はこのディレクトリを基点にバックアップの構成を行う
[ -n "$BACKUP_ROOT" ] || BACKUP_ROOT="/var/backup"

#	スナップショットデータベースのパス
#	スナップショットを管理するデータベースのパスを指定する
[ -n "$BACKUP_DB_PATH" ] || BACKUP_DB_PATH="${BACKUP_ROOT:?}/database.sqlite3"

#	スナップショットのソースディレクトリ
#	このディレクトリの中に存在する項目に対してスナップショットが作成される
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
[ -n "$BACKUP_SOURCE_PATH" ] || BACKUP_SOURCE_PATH="${BACKUP_ROOT:?}/source"

#	スナップショットの保管ディレクトリ
#	このディレクトリの中に作成したスナップショットを保管し、管理する
#	ここで指定したパスが存在しない場合、自動的にディレクトリが作成される
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
[ -n "$BACKUP_DESTINATION_PATH" ] || BACKUP_DESTINATION_PATH="${BACKUP_ROOT:?}/snapshots"

#	スナップショットの作業用一時ディレクトリ
#	このディレクトリの中にスナップショットの作成・管理に必要なデータを格納する
#	ここで指定したパスが存在しない場合、自動的にディレクトリが作成される
#	このディレクトリの内容は各タスク終了時に削除される
#	/tmpなどの一時ファイルシステムを使用してもよいが、無圧縮のスナップショットが格納できる程度のキャパシティが必要であることに注意が必要
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
[ -n "$BACKUP_WORKTMP_PATH" ] || BACKUP_WORKTMP_PATH="${BACKUP_ROOT:?}/temp"

#	スナップショット管理ルール
#	作成したスナップショットはここで指定したルールに従って管理される
#	ルールはJSONの特定の構造を持ったオブジェクトで記述する
#	空のJSON配列を渡すことでルールベースの管理を無効化し、全エントリを保管するようになる
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
[ -n "$BACKUP_KEEP_RULES" ] || BACKUP_KEEP_RULES='{}'

#	スナップショット作成前フック
#	スナップショットを作成する前に実行するスクリプトを指定する
#	ファイルが存在しない、または実行権限がないなどで実行できない場合は警告を発して処理を続行する
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
#	BACKUP_POST_SNAPSHOT_SCRIPT はこれらのスクリプトの使用を行わないことを true/false で指定する
#	trueに指定した場合、スナップショットデータベースにはnullを設定する
[ -n "$BACKUP_PRE_SNAPSHOT_SCRIPT" ] || BACKUP_PRE_SNAPSHOT_SCRIPT="${BACKUP_ROOT:?}/pre_snapshot.sh"
[ -n "$BACKUP_NO_PRE_SNAPSHOT_SCRIPT" ] || BACKUP_NO_PRE_SNAPSHOT_SCRIPT='false'

#	スナップショット作成後フック
#	スナップショットを作成する後に実行するスクリプトを指定する
#	ファイルが存在しない、または実行権限がないなどで実行できない場合は警告を発して処理を続行する
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
#	BACKUP_NO_POST_SNAPSHOT_SCRIPT はこれらのスクリプトの使用を行わないことを true/false で指定する
#	trueに指定した場合、スナップショットデータベースにはnullを設定する
[ -n "$BACKUP_POST_SNAPSHOT_SCRIPT" ] || BACKUP_POST_SNAPSHOT_SCRIPT="${BACKUP_ROOT:?}/post_snapshot.sh"
[ -n "$BACKUP_NO_POST_SNAPSHOT_SCRIPT" ] || BACKUP_NO_POST_SNAPSHOT_SCRIPT='false'

#	---- 設定項目ここまで ----

check() {
	local r
	[ -e "${BACKUP_DB_PATH:?}" ]
	r=$?; [ $r -eq 0 ] || { jq -n -c '{ error: { code: "NOT_FOUND", message: "File not found." } }'; return $r; }
	[ -f "${BACKUP_DB_PATH:?}" ]
	r=$?; [ $r -eq 0 ] || { jq -n -c '{ error: { code: "NOT_FILE", message: "Specified path exists, but not file." } }'; return $r; }
	local db_rev;	db_rev=$(sqlite3 -readonly "${BACKUP_DB_PATH:?}" "SELECT \"schema_revision\" FROM \"${BACKUP_DB_PREFIX}metadata\" WHERE \"id\"=0")
	r=$?; [ $r -eq 0 ] || { jq -n -c '{ error: { code: "DB_ERROR", message: "SQLite3 returned with error." } }'; return $r; }
	[ "${db_rev}" -le "${BACKUP_DB_SCHEMA_REVISION}" ] || { jq -n -c --argjson dbrev "$db_rev" --argjson suprev "$BACKUP_DB_SCHEMA_REVISION" '{ error: { code: "APP_OUTDATE", message: "Required update software.", db_version: $dbrev, support_version: $sup_rev } }'; return $r; }
	[ "${db_rev}" -ge "${BACKUP_DB_SCHEMA_REVISION}" ] || { jq -n -c --argjson dbrev "$db_rev" --argjson suprev "$BACKUP_DB_SCHEMA_REVISION" '{ error: { code: "DB_OUTDATE", message: "Required update database.", db_version: $dbrev, support_version: $sup_rev } }'; return $r; }
	jq -n -c '{ error: null }'
}

initialize() {
	local force
	while (( $# > 0 )); do case $1 in
		force)	force='true'; shift;;
	esac done
	if [ "$force" != 'true' ] && [ -e "${BACKUP_DB_PATH:?}" ]; then
		echo "backupctl: Database already exists. If continue anyway, re-run with force switch." >&2
		return 1
	fi
	if [ -e "${BACKUP_DB_PATH:?}" ]; then
		rm -f "${BACKUP_DB_PATH:?}" || return
	fi
	if [ -e "${BACKUP_DB_PATH:?}-journal" ]; then
		rm -f "${BACKUP_DB_PATH:?}-journal" || return
	fi
	# shellcheck disable=SC2016
	local -r BACKUP_JQ_DBSTATEMENT_INIT='( [
		"PRAGMA journal_mode = TRUNCATE",
		"BEGIN TRANSACTION",
		"CREATE TABLE \"\($db_prefix)metadata\" ( \"id\" INTEGER NOT NULL UNIQUE DEFAULT 0, \"schema_revision\" INTEGER NOT NULL, \"lock\" TEXT )",
		"CREATE TABLE \"\($db_prefix)config\" ( \"key\" TEXT NOT NULL UNIQUE, \"value\" TEXT )",
		"CREATE UNIQUE INDEX \"\($db_prefix)config-keys\" ON \"\($db_prefix)config\" ( \"key\" );",
		"CREATE TABLE \"\($db_prefix)entries\" ( \"id\" INTEGER NOT NULL UNIQUE, \"date\" NUMERIC NOT NULL, \"fname\" TEXT NOT NULL UNIQUE, \"size\" INTEGER NOT NULL, \"sha256\" TEXT NOT NULL, \"type\" TEXT NOT NULL, \"depend_id\" INTEGER, PRIMARY KEY(\"id\"), FOREIGN KEY(\"depend_id\") REFERENCES \"\($db_prefix)entries\"(\"id\") ON UPDATE CASCADE )",
		"CREATE INDEX \"\($db_prefix)entry-dates\" ON \"\($db_prefix)entries\" ( \"date\" )",
		"CREATE UNIQUE INDEX \"\($db_prefix)entry-filenames\" ON \"\($db_prefix)entries\" ( \"fname\" )",
		"CREATE TABLE \"\($db_prefix)keeprules\" ( \"name\" TEXT NOT NULL UNIQUE, \"store_type\" TEXT, \"bind_duration\" INTEGER, \"keep_entries\" INTEGER, \"keep_duration\" INTEGER )",
		"CREATE UNIQUE INDEX \"\($db_prefix)keeprule-names\" ON \"\($db_prefix)keeprules\" ( \"name\" )",
		"CREATE TABLE \"\($db_prefix)keeplist\" ( \"entry_id\" INTEGER NOT NULL, \"rule\" TEXT NOT NULL, FOREIGN KEY(\"entry_id\") REFERENCES \"\($db_prefix)entries\"(\"id\") ON UPDATE CASCADE ON DELETE CASCADE, FOREIGN KEY(\"rule\") REFERENCES \"\($db_prefix)keeprules\"(\"name\") ON UPDATE CASCADE ON DELETE CASCADE )",
		"CREATE VIEW \"\($db_prefix)keep-entries\" AS SELECT \"rule\", \"\($db_prefix)entries\".* FROM \"\($db_prefix)keeplist\" LEFT JOIN \"\($db_prefix)entries\" ON \"\($db_prefix)keeplist\".\"entry_id\"=\"\($db_prefix)entries\".\"id\" ORDER BY \"\($db_prefix)entries\".\"date\"",
		"CREATE VIEW \"\($db_prefix)keep-entry-latests\" AS SELECT * FROM \"\($db_prefix)keep-entries\" GROUP BY \"rule\" HAVING \"date\"=MAX(\"date\")",
		"CREATE TABLE \"\($db_prefix)remove_queue\" ( \"entry_id\" INTEGER NOT NULL, FOREIGN KEY(\"entry_id\") REFERENCES \"\($db_prefix)entries\"(\"id\") ON UPDATE CASCADE ON DELETE CASCADE )",
		"CREATE TABLE \"\($db_prefix)add_queue\" ( \"entry_id\" INTEGER NOT NULL, FOREIGN KEY(\"entry_id\") REFERENCES \"\($db_prefix)entries\"(\"id\") ON UPDATE CASCADE ON DELETE CASCADE )",
		"CREATE TABLE \"\($db_prefix)compression_queue\" ( \"entry_id\" INTEGER NOT NULL, FOREIGN KEY(\"entry_id\") REFERENCES \"\($db_prefix)entries\"(\"id\") ON UPDATE CASCADE ON DELETE CASCADE )",
		"INSERT INTO \"\($db_prefix)metadata\"( \"schema_revision\" ) VALUES ( \($schema_rev) )",
		(
			"INSERT INTO \"\($db_prefix)config\"( \"key\", \"value\" ) VALUES " + ( [
				"( '\''backup_source'\'', '\''\($src|tojson)'\'' )",
				"( '\''backup_destination'\'', '\''\($dest|tojson)'\'' )",
				"( '\''worktmp'\'', '\''\($wtmp|tojson)'\'' )",
				"( '\''pre_snapshot_script'\'', \(if $no_presnap == false then ("'\''"+($presnap|tojson)+"'\''") else "NULL" end) )",
				"( '\''post_snapshot_script'\'', \(if $no_postsnap == false then ("'\''"+($postsnap|tojson)+"'\''") else "NULL" end) )"
			]|join(",") )
		),
		if ($rules|length) > 0 then
		(
			"INSERT INTO \"\($db_prefix)keeprules\" ( \"name\", \"store_type\", \"bind_duration\", \"keep_entries\", \"keep_duration\" ) VALUES" +
			( $rules|to_entries|map(
				if (.value|type) != "object" then ( "backupctl: Parse error. \(.key) is not object."|halt_error ) else . end |
				.value + { name: .key } |
				"(" +
				"'\''\(.name)'\'', " +
				(if (.store_type|type)=="string" then "'\''\(.store_type)'\''" else "NULL" end) + ", " +
				(if (.bind_duration|type)=="number" then "\(.bind_duration)" else "NULL" end) + ", " +
				(if (.keep_entries|type)=="number" then "\(.keep_entries)" else "NULL" end) + ", " +
				(if (.keep_duration|type)=="number" then "\(.keep_duration)" else "NULL" end) +
				")"
			)|join(",") )
		) else empty end,
		"COMMIT TRANSACTION"
	] )|join(";")'
	local db_location
	db_location=$(dirname "${BACKUP_DB_PATH:?}")
	mkdir -p "${db_location}"
	local r_source;	r_source=${BACKUP_SOURCE_PATH#"${db_location}/"}
	local r_destination;	r_destination=${BACKUP_DESTINATION_PATH#"${db_location}/"}
	local r_wtmp;	r_wtmp=${BACKUP_WORKTMP_PATH#"${db_location}/"}
	local r_pre_snap;	r_pre_snap=${BACKUP_PRE_SNAPSHOT_SCRIPT#"${db_location}/"}
	local r_post_snap;	r_post_snap=${BACKUP_POST_SNAPSHOT_SCRIPT#"${db_location}/"}
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${BACKUP_DB_PREFIX}" --argjson schema_rev "${BACKUP_DB_SCHEMA_REVISION:?}" --arg src "${r_source:?}" --arg dest "${r_destination:?}" --arg wtmp "${r_wtmp:?}" --arg presnap "${r_pre_snap}" --argjson no_presnap "${BACKUP_NO_PRE_SNAPSHOT_SCRIPT:?}" --arg postsnap "${r_post_snap}" --argjson no_postsnap "${BACKUP_NO_POST_SNAPSHOT_SCRIPT:?}" --argjson rules "${BACKUP_KEEP_RULES:?}" "${BACKUP_JQ_DBSTATEMENT_INIT:?}") || return
	sqlite3 "${BACKUP_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rm -f "${BACKUP_DB_PATH:?}" "${BACKUP_DB_PATH:?}-journal"; return $rcode; }
}

get_config() {
	sqlite3 -readonly -json "${BACKUP_DB_PATH:?}" "SELECT \"key\", \"value\" FROM \"${BACKUP_DB_PREFIX}config\"" | jq -c 'from_entries|map_values(if type=="string" then fromjson else . end)'
}

acq_lock() {
	local lock_token=$1
	# shellcheck disable=SC2016
	local -r BACKUP_JQ_DBSTATEMENT_ACQ_LOCK='( [
		"PRAGMA journal_mode = TRUNCATE",
		"UPDATE \"\($db_prefix)metadata\" SET \"lock\"='\''\($lt)'\'' WHERE \"id\"=0 AND \"lock\" IS NULL"
	] )|join(";")'
	sqlite3 "${BACKUP_DB_PATH:?}" "$(jq -n -r --arg db_prefix "${BACKUP_DB_PREFIX}" --arg lt "${lock_token:?}" "${BACKUP_JQ_DBSTATEMENT_ACQ_LOCK:?}")" >/dev/null || return
	[ "$(sqlite3 -readonly "${BACKUP_DB_PATH:?}" "SELECT \"lock\"='${lock_token:?}' FROM \"${BACKUP_DB_PREFIX:?}metadata\" WHERE \"id\"=0")" -ne 0 ]
}

rel_lock() {
	local lock_token=$1
	# shellcheck disable=SC2016
	local -r BACKUP_JQ_DBSTATEMENT_REL_LOCK='( [
		"PRAGMA journal_mode = TRUNCATE",
		"UPDATE \"\($db_prefix)metadata\" SET \"lock\"=NULL WHERE \"id\"=0 AND \"lock\"='\''\($lt)'\''"
	] )|join(";")'
	sqlite3 "${BACKUP_DB_PATH:?}" "$(jq -n -r --arg db_prefix "${BACKUP_DB_PREFIX}" --arg lt "${lock_token:?}" "${BACKUP_JQ_DBSTATEMENT_REL_LOCK:?}")" >/dev/null || return
	[ "$(sqlite3 -readonly "${BACKUP_DB_PATH:?}" "SELECT \"lock\" IS NULL FROM \"${BACKUP_DB_PREFIX:?}metadata\" WHERE \"id\"=0")" -ne 0 ]
}

rm_lock() {
	# shellcheck disable=SC2016
	local -r BACKUP_JQ_DBSTATEMENT_RM_LOCK='( [
		"PRAGMA journal_mode = TRUNCATE",
		"UPDATE \"\($db_prefix)metadata\" SET \"lock\"=NULL WHERE \"id\"=0"
	] )|join(";")'
	sqlite3 "${BACKUP_DB_PATH:?}" "$(jq -n -r --arg db_prefix "${BACKUP_DB_PREFIX}" "${BACKUP_JQ_DBSTATEMENT_RM_LOCK:?}")" >/dev/null || return
	[ "$(sqlite3 -readonly "${BACKUP_DB_PATH:?}" "SELECT \"lock\" IS NULL FROM \"${BACKUP_DB_PREFIX:?}metadata\" WHERE \"id\"=0")" -ne 0 ]
}

do_lock() {
	local lock_token=$1
	shift
	# shellcheck disable=SC2016
	local -r BACKUP_JQ_DBSTATEMENT_GET_LOCK='( [
		"SELECT \"lock\"='\''\($lt)'\'' FROM \"\($db_prefix)metadata\" WHERE \"id\"=0"
	] )|join(";")'
	result=$(sqlite3 -readonly "${BACKUP_DB_PATH:?}" "$(jq -n -r --arg db_prefix "${BACKUP_DB_PREFIX}" --arg lt "${lock_token:?}" "${BACKUP_JQ_DBSTATEMENT_GET_LOCK:?}")") || return
	[ "${result:?}" -ne 0 ] || return
	"$@"
}

create_snapshot() {
	local db_location;	db_location=$(cd "$(dirname "${BACKUP_DB_PATH:?}")" && pwd) || return
	local backup_source;	backup_source=$(get_config | jq -r --arg db_location "${db_location:?}" '.backup_source|if startswith("/") then . else ("\($db_location)/" + .) end') || return
	(cd "${backup_source:?}") || return
	local backup_destination;	backup_destination=$(get_config | jq -r --arg db_location "${db_location:?}" '.backup_destination|if startswith("/") then . else ("\($db_location)/" + .) end') || return
	local worktmp;	worktmp=$(get_config | jq -r --arg db_location "${db_location:?}" '.worktmp|if startswith("/") then . else ("\($db_location)/" + .) end') || return
	local pre_snapshot_script;	pre_snapshot_script=$(get_config | jq -r --arg db_location "${db_location:?}" '.pre_snapshot_script|strings|if (type!="string") or startswith("/") then . else ("\($db_location)/" + .) end') || return
	local post_snapshot_script;	post_snapshot_script=$(get_config | jq -r --arg db_location "${db_location:?}" '.post_snapshot_script|strings|if (type!="string") or startswith("/") then . else ("\($db_location)/" + .) end') || return
	local create_time
	if [ -n "$(date +%N)" ]; then
		create_time=$(date +%s.%N)
	else
		create_time=$(date +%s)
	fi
	local snapshot_filename;	snapshot_filename="snapshot_$(date --date="@${create_time:?}" +%Y%m%d_%H%M%S_%N).tar"
	local lock_code;	lock_code=$(cat <(echo "CREATE:${snapshot_filename:?}:") <(head --bytes=8 -q /dev/urandom) | sha256sum -b - | awk '{ print $1 }') || return
	acq_lock "${lock_code:?}" || return
	if [ -e "${worktmp:?}" ]; then
		do_lock "${lock_code:?}" rm -rf "${worktmp:?}/*"
	else
		do_lock "${lock_code:?}" mkdir -p "${worktmp:?}"
	fi
	if [ -n "${pre_snapshot_script}" ] && [ -x "${pre_snapshot_script:?}" ]; then
		(do_lock "${lock_code:?}" "${pre_snapshot_script:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	fi
	(cd "${backup_source:?}" && do_lock "${lock_code:?}" tar -cf "${worktmp:?}/${snapshot_filename:?}" ./*) || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	if [ -n "${post_snapshot_script}" ] && [ -x "${post_snapshot_script:?}" ]; then
		(do_lock "${lock_code:?}" "${post_snapshot_script:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	fi
	local size;	size=$(do_lock "${lock_code:?}" wc -c "${worktmp:?}/${snapshot_filename:?}" | awk '{ print $1 }') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	local checksum;	checksum=$(do_lock "${lock_code:?}" sha256sum -b "${worktmp:?}/${snapshot_filename:?}" | awk '{ print $1 }') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	[ -e "${backup_destination:?}" ] || { do_lock "${lock_code:?}" mkdir -p "${backup_destination:?}"; }
	do_lock "${lock_code:?}" mv --no-clobber --target-directory="${backup_destination:?}/" "${worktmp:?}/${snapshot_filename:?}" || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	# shellcheck disable=SC2016
	local -r BACKUP_JQ_DBSTATEMENT_CREATE='( [
		"PRAGMA journal_mode = TRUNCATE",
		"BEGIN TRANSACTION",
		"INSERT INTO \"\($db_prefix)entries\"( \"date\", \"fname\", \"size\", \"sha256\", \"type\", \"depend_id\" ) VALUES ( \($date), '\''\($fname)'\'', \($size), '\''\($sha256)'\'', '\''plain'\'', NULL )",
		"INSERT INTO \"\($db_prefix)add_queue\"( \"entry_id\" ) VALUES ( ( SELECT MAX(\"id\") FROM \"\($db_prefix)entries\" ) )",
		"COMMIT TRANSACTION"
	] )|join(";")'
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${BACKUP_DB_PREFIX}" --argjson date "${create_time:?}" --arg fname "${snapshot_filename:?}" --argjson size "${size:?}" --arg sha256 "${checksum:?}" "${BACKUP_JQ_DBSTATEMENT_CREATE:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	sqlite3 "${BACKUP_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	rel_lock "${lock_code:?}"
}

update_keeplist() {
	local lock_code;	lock_code=$(cat <(echo "UPDATE:") <(head --bytes=8 -q /dev/urandom) | sha256sum -b - | awk '{ print $1 }') || return
	acq_lock "${lock_code:?}" || return
	local rules;	rules=$(do_lock "${lock_code:?}" sqlite3 -json -readonly "${BACKUP_DB_PATH:?}" "SELECT * FROM \"${BACKUP_DB_PREFIX}keeprules\"") || return
	[ -n "$rules" ] || rules='[]'
	# shellcheck disable=SC2016
	local -r BACKUP_JQ_DBSTATEMENT_UPDATE='( [
		"PRAGMA journal_mode = TRUNCATE",
		"BEGIN TRANSACTION"
	] + (
		$rules|map( if (.keep_entries|type) == "number" then "DELETE FROM \"\($db_prefix)keeplist\" WHERE \"rule\"='\''\(.name)'\'' AND \"entry_id\" IN ( SELECT \"id\" FROM \"\($db_prefix)keep-entries\" WHERE \"rule\"='\''\(.name)'\'' ORDER BY \"date\" ASC LIMIT ( SELECT MAX(COUNT(*)-\(.keep_entries), 0) FROM \"\($db_prefix)keep-entries\" WHERE \"rule\"='\''\(.name)'\'') )" else empty end )
	) + (
		$rules|map( if (.keep_duration|type) == "number" then "DELETE FROM \"\($db_prefix)keeplist\" WHERE \"rule\"='\''\(.name)'\'' AND \"entry_id\" IN ( SELECT \"id\" FROM \"\($db_prefix)keep-entries\" WHERE \"date\" < ( SELECT (\"date\"-\(.keep_duration)) FROM \"\($db_prefix)keep-entry-latests\" WHERE \"rule\"='\''\(.name)'\'' ) )" else empty end )
	) + (
		$rules|if length > 0 then [ "INSERT INTO \"\($db_prefix)remove_queue\"(\"entry_id\") SELECT \"id\" FROM \"\($db_prefix)entries\" WHERE \"id\" NOT IN ( SELECT \"entry_id\" FROM \"\($db_prefix)keeplist\" UNION SELECT \"entry_id\" FROM \"\($db_prefix)remove_queue\" )", "INSERT INTO \"\($db_prefix)compression_queue\"(\"entry_id\") SELECT DISTINCT \"entry_id\" FROM \"\($db_prefix)keeplist\" INNER JOIN \"\($db_prefix)keeprules\" ON \"\($db_prefix)keeprules\".\"name\"=\"\($db_prefix)keeplist\".\"rule\" WHERE \"\($db_prefix)keeprules\".\"store_type\" IS NOT NULL AND \"entry_id\" NOT IN ( SELECT \"\($db_prefix)keep-entry-latests\".\"id\" FROM \"\($db_prefix)keep-entry-latests\" UNION SELECT \"entry_id\" FROM \"\($db_prefix)compression_queue\" )" ] else [] end
	) + [
		"COMMIT TRANSACTION"
	] )|join(";")'
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${BACKUP_DB_PREFIX}" --argjson rules "${rules:?}" "${BACKUP_JQ_DBSTATEMENT_UPDATE:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	sqlite3 "${BACKUP_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	rel_lock "${lock_code:?}"
}

process_add_queue_item() {
	local entry_id=$1
	[ "$(sqlite3 -readonly "${BACKUP_DB_PATH:?}" "SELECT COUNT(*) FROM \"${BACKUP_DB_PREFIX}add_queue\" WHERE \"entry_id\"=${entry_id:?}")" -gt 0 ] || { echo "snapshotctl: Not exist entry ${entry_id} from add queue" >&2; return 1; }

	local lock_code;	lock_code=$(cat <(echo "ADD:${entry_id:?}:") <(head --bytes=8 -q /dev/urandom) | sha256sum -b - | awk '{ print $1 }') || return
	acq_lock "${lock_code:?}" || return
	local entry;	entry=$(do_lock "${lock_code:?}" sqlite3 -json -readonly "${BACKUP_DB_PATH:?}" "SELECT \"${BACKUP_DB_PREFIX}entries\".* FROM \"${BACKUP_DB_PREFIX}add_queue\" LEFT JOIN \"${BACKUP_DB_PREFIX}entries\" ON \"${BACKUP_DB_PREFIX}add_queue\".\"entry_id\"=\"${BACKUP_DB_PREFIX}entries\".\"id\"" | jq -c '.[0]') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	local latest_entry;	latest_entry=$(do_lock "${lock_code:?}" sqlite3 -json -readonly "${BACKUP_DB_PATH:?}" "SELECT * FROM \"${BACKUP_DB_PREFIX}keep-entry-latests\"" | jq -c 'map({ key: .rule, value: del(.rule) })|from_entries') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	[ -n "${latest_entry}" ] || latest_entry='{}'
	local rules;	rules=$(do_lock "${lock_code:?}" sqlite3 -json -readonly "${BACKUP_DB_PATH:?}" "SELECT * FROM \"${BACKUP_DB_PREFIX}keeprules\"" | jq -c --argjson latest "${latest_entry:?}" 'map(.name as $rule_name|. + { latest: ($latest|.[$rule_name]) })') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	# shellcheck disable=SC2016
	local -r BACKUP_JQ_DBSTATEMENT_PROCESS_ADD_ITEM='( [
		"PRAGMA journal_mode = TRUNCATE",
		"BEGIN TRANSACTION"
	] +
	( $rules|map(
		if ((.bind_duration|type) == "null") or ((.latest|type) == "null") or (($entry|.date) >= (.latest.date + .bind_duration)) then "INSERT INTO \"\($db_prefix)keeplist\"( \"entry_id\", \"rule\" ) VALUES ( \($entry|.id), '\''\(.name)'\'' )" else empty end
	) ) + [
		"DELETE FROM \"\($db_prefix)add_queue\" WHERE \"entry_id\"=\($entry|.id)",
		"COMMIT TRANSACTION"
	] )|join(";")'
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${BACKUP_DB_PREFIX}" --argjson rules "${rules:?}" --argjson entry "${entry:?}" "${BACKUP_JQ_DBSTATEMENT_PROCESS_ADD_ITEM:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	sqlite3 "${BACKUP_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	rel_lock "${lock_code:?}"
}

process_add_queue() {
	local add_queue;	add_queue=$(sqlite3 -json -readonly "${BACKUP_DB_PATH:?}" "SELECT \"entry_id\" FROM \"${BACKUP_DB_PREFIX}add_queue\" LEFT JOIN \"${BACKUP_DB_PREFIX}entries\" ON \"${BACKUP_DB_PREFIX}add_queue\".\"entry_id\"=\"${BACKUP_DB_PREFIX}entries\".\"id\" ORDER BY \"${BACKUP_DB_PREFIX}entries\".\"date\" ASC" | jq -c 'map(.entry_id)') || return
	[ -n "$add_queue" ] || add_queue='[]'
	for entry_id in $(jq -n -c --argjson add_queue "$add_queue" '$add_queue|.[]'); do
		process_add_queue_item "$entry_id" || return
	done
}

process_remove_queue_item() {
	local entry_id=$1
	[ "$(sqlite3 -readonly "${BACKUP_DB_PATH:?}" "SELECT COUNT(*) FROM \"${BACKUP_DB_PREFIX}remove_queue\" WHERE \"entry_id\"=${entry_id:?}")" -gt 0 ] || { echo "Snapshotctl: Not exist entry ${entry_id} from remove queue" >&2; return 1; }

	local db_location;	db_location=$(cd "$(dirname "${BACKUP_DB_PATH:?}")" && pwd) || return
	local backup_destination;	backup_destination=$(get_config | jq -r --arg db_location "${db_location:?}" '.backup_destination|if startswith("/") then . else ("\($db_location)/" + .) end') || return
	local item_path;	item_path=$(sqlite3 -readonly "${BACKUP_DB_PATH:?}" "SELECT ('${backup_destination:?}/' || \"fname\") FROM \"${BACKUP_DB_PREFIX}entries\" WHERE \"id\"=${entry_id:?}") || return
	local lock_code;	lock_code=$(cat <(echo "REMOVE:${entry_id:?}:") <(head --bytes=8 -q /dev/urandom) | sha256sum -b - | awk '{ print $1 }') || return
	acq_lock "${lock_code:?}" || return
	if [ -e "${item_path:?}" ]; then
		rm -f "${item_path:?}" || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	fi
	# shellcheck disable=SC2016
	local -r BACKUP_JQ_DBSTATEMENT_PROCESS_REMOVE_ITEM='( [
		"PRAGMA journal_mode = TRUNCATE",
		"DELETE FROM \"\($db_prefix)remove_queue\" WHERE \"entry_id\"=\($entry_id)",
		"DELETE FROM \"\($db_prefix)entries\" WHERE \"id\"=\($entry_id)"
	] )|join(";")'
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${BACKUP_DB_PREFIX}" --arg entry_id "${entry_id:?}" "${BACKUP_JQ_DBSTATEMENT_PROCESS_REMOVE_ITEM:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	sqlite3 "${BACKUP_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	rel_lock "${lock_code:?}"
}

process_remove_queue() {
	local remove_queue;	remove_queue=$(sqlite3 -json -readonly "${BACKUP_DB_PATH:?}" "SELECT DISTINCT \"entry_id\" FROM \"${BACKUP_DB_PREFIX}remove_queue\"" | jq -c 'map(.entry_id)') || return
	[ -n "$remove_queue" ] || remove_queue='[]'
	for item in $(jq -n -r --argjson remove_queue "$remove_queue" '$remove_queue|.[]'); do
		process_remove_queue_item "$item" || return
	done
}

command_help() {
	cat <<-__EOF
	$0
	Snapshot create/management utility

	usage:
	  $0 command ...
	  $0 --help

	commands
	  initialize
	    Initialize database.
	  check
	    Check system or database.
	  create
	    Create snapshot.
	  help
	    Show this help.

	options
	  --help | -h
	    Show this help and exit.
	__EOF
}

command_initialize() {
	help() {
		cat <<-__EOF
		$0 initialize
		Initialize database

		usage: $0 initialize [<options>]

		options
		  --force | -f
		    Force operation.
		    This option will remove (and recreate) database if already exists.
		  --help | -h
		    Show this help and exit.
		__EOF
	}
	local opt_force=
	local opt_help=
	while (( $# > 0 )); do case $1 in
		--help)		help;	return;;
		--force)	opt_force='true';	shift;;
		--*)		echo "Invalid option: $1" >&2;	echo "Type \"$0 initialize --help\" for more help." >&2;	return 1;;
		-*)
			if [[ $1 =~ f ]]; then opt_force='true'; fi
			if [[ $1 =~ h ]]; then opt_help='true'; fi
			if [ -n "$opt_help" ]; then help; break; fi
			shift;;
		*)			echo "Warning: Extra argument $1" >&2;	shift;;
	esac done
	if [ -z "$opt_force" ]; then
		initialize
	else
		initialize force
	fi
}

command_create() {
	help() {
		cat <<-__EOF
		$0 create
		Create snapshot

		usage:
		  $0 create
		  $0 create --help

		options
		  --create-only
		    Not update keeplist.
		  --help | -h
		    Show this help and exit.
		__EOF
	}
	local opt_help=
	local opt_create_only=
	while (( $# > 0 )); do case $1 in
		--help)		help;	return;;
		--create-only)	shift;	opt_create_only='true';;
		--*)		echo "Invalid option: $1" >&2;	echo "Type \"$0 initialize --help\" for more help." >&2;	return 1;;
		-*)
			if [[ $1 =~ h ]]; then opt_help='true'; fi
			if [ -n "$opt_help" ]; then help; break; fi
			shift;;
		*)			echo "Warning: Extra argument $1" >&2;	shift;;
	esac done

	check | jq '.error|if type != "null" then ("snapshotctl: Error reported when database checking\n\(.code): \(.message)"|halt_error(1)) else empty end' >/dev/null || return
	create_snapshot || return
	process_add_queue || return
	[ -z "$opt_create_only" ] || return 0
	update_keeplist || return
}

command_update() {
	help() {
		cat <<-__EOF
		$0 update
		Update snapshot database

		usage:
		  $0 update
		  $0 create --help

		options
		  --help | -h
		    Show this help and exit.
		__EOF
	}
	local opt_help=
	while (( $# > 0 )); do case $1 in
		--help)		help;	return;;
		--*)		echo "Invalid option: $1" >&2;	echo "Type \"$0 initialize --help\" for more help." >&2;	return 1;;
		-*)
			if [[ $1 =~ h ]]; then opt_help='true'; fi
			if [ -n "$opt_help" ]; then help; break; fi
			shift;;
		*)			echo "Warning: Extra argument $1" >&2;	shift;;
	esac done

	check | jq '.error|if type != "null" then ("snapshotctl: Error reported when database checking\n\(.code): \(.message)"|halt_error(1)) else empty end' >/dev/null || return
	process_add_queue || return
	update_keeplist || return
	process_remove_queue || return
}

command_clean() {
	echo "Not implemented yet" >&2
	return 255
}

command_check() {
	echo "Not implemented yet" >&2
	return 255
}

[ $# -gt 0 ] || { echo "No commands given." >&2; echo "Type \"$0 help\" for more help." >&2; exit 1; }

while (( $# > 0 )); do case $1 in
	initialize)	shift;	command_initialize "$@";	exit;;
	create)		shift;	command_create "$@";	exit;;
	update)		shift;	command_update "$@";	exit;;
	clean)		shift;	command_clean "$@";	exit;;
	check)		shift;	command_check "$@";	exit;;
	help)		shift;	command_help;	break;;
	--help)		shift;	command_help;	break;;
	--*)		echo "Invalid option: $1" >&2;	echo "Type \"$0 help\" for more help." >&2;	exit 1;;
	-*)
		if [[ $1 =~ h ]]; then opt_help='true'; fi
		if [ -n "$opt_help" ]; then command_help; break; fi
		shift;;
	*)			echo "Invalid argument: $1" >&2;	echo "Type \"$0 help\" for more help." >&2;	exit 1;;
esac done
