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

# SNAPSHOTCTL_BIN_LOCATION="$(cd "$(dirname "$0")" && pwd)" || exit
# readonly SNAPSHOTCTL_BIN_LOCATION
readonly SNAPSHOTCTL_DB_PREFIX='backup-'
readonly SNAPSHOTCTL_DB_SCHEMA_REVISION=1

#	---- 設定項目 ----
#	スクリプトの動作を変更する環境変数とそのデフォルトの値
#	基本的にはここを変更するの**ではなく**、環境変数を設定してこのスクリプトを実行することを推奨する

#	スナップショット管理ディレクトリのルート
#	スナップショットを管理するためのデータベース等を配置する基点となるディレクトリ
#	設定を省略した場合はこのディレクトリを基点にバックアップの構成を行う
[ -n "$SNAPSHOTCTL_ROOT" ] || SNAPSHOTCTL_ROOT="/var/backup"

#	スナップショットデータベースのパス
#	スナップショットを管理するデータベースのパスを指定する
[ -n "$SNAPSHOTCTL_DB_PATH" ] || SNAPSHOTCTL_DB_PATH="${SNAPSHOTCTL_ROOT:?}/database.sqlite3"

#	スナップショットのソースディレクトリ
#	このディレクトリの中に存在する項目に対してスナップショットが作成される
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
[ -n "$SNAPSHOTCTL_SOURCE_PATH" ] || SNAPSHOTCTL_SOURCE_PATH="${SNAPSHOTCTL_ROOT:?}/source"

#	スナップショットの保管ディレクトリ
#	このディレクトリの中に作成したスナップショットを保管し、管理する
#	ここで指定したパスが存在しない場合、自動的にディレクトリが作成される
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
[ -n "$SNAPSHOTCTL_DESTINATION_PATH" ] || SNAPSHOTCTL_DESTINATION_PATH="${SNAPSHOTCTL_ROOT:?}/snapshots"

#	スナップショットの作業用一時ディレクトリ
#	このディレクトリの中にスナップショットの作成・管理に必要なデータを格納する
#	ここで指定したパスが存在しない場合、自動的にディレクトリが作成される
#	このディレクトリの内容は各タスク終了時に削除される
#	/tmpなどの一時ファイルシステムを使用してもよいが、無圧縮のスナップショットが格納できる程度のキャパシティが必要であることに注意が必要
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
[ -n "$SNAPSHOTCTL_WORKTMP_PATH" ] || SNAPSHOTCTL_WORKTMP_PATH="${SNAPSHOTCTL_ROOT:?}/temp"

#	スナップショット管理ルール
#	作成したスナップショットはここで指定したルールに従って管理される
#	ルールはJSONの特定の構造を持ったオブジェクトで記述する
#	空のJSON配列を渡すことでルールベースの管理を無効化し、全エントリを保管するようになる
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
[ -n "$SNAPSHOTCTL_KEEP_RULES" ] || SNAPSHOTCTL_KEEP_RULES='{}'

#	スナップショット作成前フック
#	スナップショットを作成する前に実行するスクリプトを指定する
#	ファイルが存在しない、または実行権限がないなどで実行できない場合は警告を発して処理を続行する
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
#	SNAPSHOTCTL_POST_SNAPSHOT_SCRIPT はこれらのスクリプトの使用を行わないことを true/false で指定する
#	trueに指定した場合、スナップショットデータベースにはnullを設定する
[ -n "$SNAPSHOTCTL_PRE_SNAPSHOT_SCRIPT" ] || SNAPSHOTCTL_PRE_SNAPSHOT_SCRIPT="${SNAPSHOTCTL_ROOT:?}/pre_snapshot.sh"
[ -n "$SNAPSHOTCTL_NO_PRE_SNAPSHOT_SCRIPT" ] || SNAPSHOTCTL_NO_PRE_SNAPSHOT_SCRIPT='false'

#	スナップショット作成後フック
#	スナップショットを作成する後に実行するスクリプトを指定する
#	ファイルが存在しない、または実行権限がないなどで実行できない場合は警告を発して処理を続行する
#	スナップショットデータベース作成時にこの設定が適用され、スナップショットデータベースに保存される
#	SNAPSHOTCTL_NO_POST_SNAPSHOT_SCRIPT はこれらのスクリプトの使用を行わないことを true/false で指定する
#	trueに指定した場合、スナップショットデータベースにはnullを設定する
[ -n "$SNAPSHOTCTL_POST_SNAPSHOT_SCRIPT" ] || SNAPSHOTCTL_POST_SNAPSHOT_SCRIPT="${SNAPSHOTCTL_ROOT:?}/post_snapshot.sh"
[ -n "$SNAPSHOTCTL_NO_POST_SNAPSHOT_SCRIPT" ] || SNAPSHOTCTL_NO_POST_SNAPSHOT_SCRIPT='false'

#	---- 設定項目ここまで ----

check() {
	local r
	[ -e "${SNAPSHOTCTL_DB_PATH:?}" ]
	r=$?; [ $r -eq 0 ] || { jq -n -c '{ error: { code: "NOT_FOUND", message: "File not found." } }'; return $r; }
	[ -f "${SNAPSHOTCTL_DB_PATH:?}" ]
	r=$?; [ $r -eq 0 ] || { jq -n -c '{ error: { code: "NOT_FILE", message: "Specified path exists, but not file." } }'; return $r; }
	local db_rev;	db_rev=$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"schema_revision\" FROM \"${SNAPSHOTCTL_DB_PREFIX}metadata\" WHERE \"id\"=0")
	r=$?; [ $r -eq 0 ] || { jq -n -c '{ error: { code: "DB_ERROR", message: "SQLite3 returned with error." } }'; return $r; }
	[ "${db_rev}" -le "${SNAPSHOTCTL_DB_SCHEMA_REVISION}" ] || { jq -n -c --argjson dbrev "$db_rev" --argjson suprev "$SNAPSHOTCTL_DB_SCHEMA_REVISION" '{ error: { code: "APP_OUTDATE", message: "Required update software.", db_version: $dbrev, support_version: $sup_rev } }'; return $r; }
	[ "${db_rev}" -ge "${SNAPSHOTCTL_DB_SCHEMA_REVISION}" ] || { jq -n -c --argjson dbrev "$db_rev" --argjson suprev "$SNAPSHOTCTL_DB_SCHEMA_REVISION" '{ error: { code: "DB_OUTDATE", message: "Required update database.", db_version: $dbrev, support_version: $sup_rev } }'; return $r; }
	jq -n -c '{ error: null }'
}

initialize() {
	local force
	while (( $# > 0 )); do case $1 in
		force)	force='true'; shift;;
	esac done
	if [ "$force" != 'true' ] && [ -e "${SNAPSHOTCTL_DB_PATH:?}" ]; then
		echo "backupctl: Database already exists. If continue anyway, re-run with force switch." >&2
		return 1
	fi
	if [ -e "${SNAPSHOTCTL_DB_PATH:?}" ]; then
		rm -f "${SNAPSHOTCTL_DB_PATH:?}" || return
	fi
	if [ -e "${SNAPSHOTCTL_DB_PATH:?}-journal" ]; then
		rm -f "${SNAPSHOTCTL_DB_PATH:?}-journal" || return
	fi
	# shellcheck disable=SC2016
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_INIT='( [
		"PRAGMA journal_mode = TRUNCATE",
		"BEGIN TRANSACTION",
		"CREATE TABLE \"\($db_prefix)metadata\" ( \"id\" INTEGER NOT NULL UNIQUE DEFAULT 0, \"schema_revision\" INTEGER NOT NULL, \"lock\" TEXT )",
		"CREATE TABLE \"\($db_prefix)config\" ( \"key\" TEXT NOT NULL UNIQUE, \"value\" TEXT )",
		"CREATE UNIQUE INDEX \"\($db_prefix)config-keys\" ON \"\($db_prefix)config\" ( \"key\" );",
		"CREATE TABLE \"\($db_prefix)entries\" ( \"id\" INTEGER NOT NULL UNIQUE, \"date\" NUMERIC NOT NULL, \"fname\" TEXT NOT NULL UNIQUE, \"size\" INTEGER NOT NULL, \"sha256\" TEXT NOT NULL, \"type\" TEXT NOT NULL, PRIMARY KEY(\"id\") )",
		"CREATE INDEX \"\($db_prefix)entry-dates\" ON \"\($db_prefix)entries\" ( \"date\" )",
		"CREATE UNIQUE INDEX \"\($db_prefix)entry-filenames\" ON \"\($db_prefix)entries\" ( \"fname\" )",
		"CREATE TABLE \"\($db_prefix)keeprules\" ( \"name\" TEXT NOT NULL UNIQUE, \"store_type\" TEXT, \"bind_duration\" INTEGER, \"keep_entries\" INTEGER, \"keep_duration\" INTEGER )",
		"CREATE UNIQUE INDEX \"\($db_prefix)keeprule-names\" ON \"\($db_prefix)keeprules\" ( \"name\" )",
		"CREATE TABLE \"\($db_prefix)keeplist\" ( \"entry_id\" INTEGER NOT NULL, \"rule\" TEXT NOT NULL, FOREIGN KEY(\"entry_id\") REFERENCES \"\($db_prefix)entries\"(\"id\") ON UPDATE CASCADE ON DELETE CASCADE, FOREIGN KEY(\"rule\") REFERENCES \"\($db_prefix)keeprules\"(\"name\") ON UPDATE CASCADE ON DELETE CASCADE )",
		"CREATE VIEW \"\($db_prefix)keep-entries\" AS SELECT \"rule\", \"\($db_prefix)entries\".* FROM \"\($db_prefix)keeplist\" LEFT JOIN \"\($db_prefix)entries\" ON \"\($db_prefix)keeplist\".\"entry_id\"=\"\($db_prefix)entries\".\"id\" ORDER BY \"\($db_prefix)entries\".\"date\"",
		"CREATE VIEW \"\($db_prefix)keep-entry-latests\" AS SELECT * FROM \"\($db_prefix)keep-entries\" GROUP BY \"rule\" HAVING \"date\"=MAX(\"date\")",
		"CREATE TABLE \"\($db_prefix)remove_queue\" ( \"entry_id\" INTEGER NOT NULL, FOREIGN KEY(\"entry_id\") REFERENCES \"\($db_prefix)entries\"(\"id\") ON UPDATE CASCADE ON DELETE CASCADE )",
		"CREATE TABLE \"\($db_prefix)add_queue\" ( \"entry_id\" INTEGER NOT NULL, FOREIGN KEY(\"entry_id\") REFERENCES \"\($db_prefix)entries\"(\"id\") ON UPDATE CASCADE ON DELETE CASCADE )",
		"CREATE TABLE \"\($db_prefix)filter_queue\" ( \"entry_id\" INTEGER NOT NULL, FOREIGN KEY(\"entry_id\") REFERENCES \"\($db_prefix)entries\"(\"id\") ON UPDATE CASCADE ON DELETE CASCADE )",
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
	db_location=$(dirname "${SNAPSHOTCTL_DB_PATH:?}")
	mkdir -p "${db_location}"
	local r_source;	r_source=${SNAPSHOTCTL_SOURCE_PATH#"${db_location}/"}
	local r_destination;	r_destination=${SNAPSHOTCTL_DESTINATION_PATH#"${db_location}/"}
	local r_wtmp;	r_wtmp=${SNAPSHOTCTL_WORKTMP_PATH#"${db_location}/"}
	local r_pre_snap;	r_pre_snap=${SNAPSHOTCTL_PRE_SNAPSHOT_SCRIPT#"${db_location}/"}
	local r_post_snap;	r_post_snap=${SNAPSHOTCTL_POST_SNAPSHOT_SCRIPT#"${db_location}/"}
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" --argjson schema_rev "${SNAPSHOTCTL_DB_SCHEMA_REVISION:?}" --arg src "${r_source:?}" --arg dest "${r_destination:?}" --arg wtmp "${r_wtmp:?}" --arg presnap "${r_pre_snap}" --argjson no_presnap "${SNAPSHOTCTL_NO_PRE_SNAPSHOT_SCRIPT:?}" --arg postsnap "${r_post_snap}" --argjson no_postsnap "${SNAPSHOTCTL_NO_POST_SNAPSHOT_SCRIPT:?}" --argjson rules "${SNAPSHOTCTL_KEEP_RULES:?}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_INIT:?}") || return
	sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rm -f "${SNAPSHOTCTL_DB_PATH:?}" "${SNAPSHOTCTL_DB_PATH:?}-journal"; return $rcode; }
}

get_config() {
	sqlite3 -readonly -json "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"key\", \"value\" FROM \"${SNAPSHOTCTL_DB_PREFIX}config\"" | jq -c 'from_entries|map_values(if type=="string" then fromjson else . end)'
}

acq_lock() {
	local lock_token=$1
	# shellcheck disable=SC2016
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_ACQ_LOCK='( [
		"PRAGMA journal_mode = TRUNCATE",
		"UPDATE \"\($db_prefix)metadata\" SET \"lock\"='\''\($lt)'\'' WHERE \"id\"=0 AND \"lock\" IS NULL"
	] )|join(";")'
	sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" --arg lt "${lock_token:?}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_ACQ_LOCK:?}")" >/dev/null || return
	[ "$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"lock\"='${lock_token:?}' FROM \"${SNAPSHOTCTL_DB_PREFIX:?}metadata\" WHERE \"id\"=0")" -ne 0 ]
}

rel_lock() {
	local lock_token=$1
	# shellcheck disable=SC2016
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_REL_LOCK='( [
		"PRAGMA journal_mode = TRUNCATE",
		"UPDATE \"\($db_prefix)metadata\" SET \"lock\"=NULL WHERE \"id\"=0 AND \"lock\"='\''\($lt)'\''"
	] )|join(";")'
	sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" --arg lt "${lock_token:?}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_REL_LOCK:?}")" >/dev/null || return
	[ "$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"lock\" IS NULL FROM \"${SNAPSHOTCTL_DB_PREFIX:?}metadata\" WHERE \"id\"=0")" -ne 0 ]
}

rm_lock() {
	# shellcheck disable=SC2016
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_RM_LOCK='( [
		"PRAGMA journal_mode = TRUNCATE",
		"UPDATE \"\($db_prefix)metadata\" SET \"lock\"=NULL WHERE \"id\"=0"
	] )|join(";")'
	sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_RM_LOCK:?}")" >/dev/null || return
	[ "$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"lock\" IS NULL FROM \"${SNAPSHOTCTL_DB_PREFIX:?}metadata\" WHERE \"id\"=0")" -ne 0 ]
}

do_lock() {
	local lock_token=$1
	shift
	# shellcheck disable=SC2016
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_GET_LOCK='( [
		"SELECT \"lock\"='\''\($lt)'\'' FROM \"\($db_prefix)metadata\" WHERE \"id\"=0"
	] )|join(";")'
	result=$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" --arg lt "${lock_token:?}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_GET_LOCK:?}")") || return
	[ "${result:?}" -ne 0 ] || return
	"$@"
}

create_snapshot() {
	local db_location;	db_location=$(cd "$(dirname "${SNAPSHOTCTL_DB_PATH:?}")" && pwd) || return
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
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_CREATE='( [
		"PRAGMA journal_mode = TRUNCATE",
		"BEGIN TRANSACTION",
		"INSERT INTO \"\($db_prefix)entries\"( \"date\", \"fname\", \"size\", \"sha256\", \"type\" ) VALUES ( \($date), '\''\($fname)'\'', \($size), '\''\($sha256)'\'', '\''plain'\'' )",
		"INSERT INTO \"\($db_prefix)add_queue\"( \"entry_id\" ) VALUES ( ( SELECT MAX(\"id\") FROM \"\($db_prefix)entries\" ) )",
		"COMMIT TRANSACTION"
	] )|join(";")'
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" --argjson date "${create_time:?}" --arg fname "${snapshot_filename:?}" --argjson size "${size:?}" --arg sha256 "${checksum:?}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_CREATE:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	rel_lock "${lock_code:?}"
}

update_keeplist() {
	local lock_code;	lock_code=$(cat <(echo "UPDATE:") <(head --bytes=8 -q /dev/urandom) | sha256sum -b - | awk '{ print $1 }') || return
	acq_lock "${lock_code:?}" || return
	local rules;	rules=$(do_lock "${lock_code:?}" sqlite3 -json -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT * FROM \"${SNAPSHOTCTL_DB_PREFIX}keeprules\"") || return
	[ -n "$rules" ] || rules='[]'
	# shellcheck disable=SC2016
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_UPDATE='( [
		"PRAGMA journal_mode = TRUNCATE",
		"BEGIN TRANSACTION"
	] + (
		$rules|map( if (.keep_entries|type) == "number" then "DELETE FROM \"\($db_prefix)keeplist\" WHERE \"rule\"='\''\(.name)'\'' AND \"entry_id\" IN ( SELECT \"id\" FROM \"\($db_prefix)keep-entries\" WHERE \"rule\"='\''\(.name)'\'' ORDER BY \"date\" ASC LIMIT ( SELECT MAX(COUNT(*)-\(.keep_entries), 0) FROM \"\($db_prefix)keep-entries\" WHERE \"rule\"='\''\(.name)'\'') )" else empty end )
	) + (
		$rules|map( if (.keep_duration|type) == "number" then "DELETE FROM \"\($db_prefix)keeplist\" WHERE \"rule\"='\''\(.name)'\'' AND \"entry_id\" IN ( SELECT \"id\" FROM \"\($db_prefix)keep-entries\" WHERE \"date\" < ( SELECT (\"date\"-\(.keep_duration)) FROM \"\($db_prefix)keep-entry-latests\" WHERE \"rule\"='\''\(.name)'\'' ) )" else empty end )
	) + (
		$rules|if length > 0 then [ "INSERT INTO \"\($db_prefix)remove_queue\"(\"entry_id\") SELECT \"id\" FROM \"\($db_prefix)entries\" WHERE \"id\" NOT IN ( SELECT \"entry_id\" FROM \"\($db_prefix)keeplist\" UNION SELECT \"entry_id\" FROM \"\($db_prefix)remove_queue\" )", "INSERT INTO \"\($db_prefix)filter_queue\"(\"entry_id\") SELECT DISTINCT \"entry_id\" FROM \"\($db_prefix)keeplist\" INNER JOIN \"\($db_prefix)keeprules\" ON \"\($db_prefix)keeprules\".\"name\"=\"\($db_prefix)keeplist\".\"rule\" WHERE \"\($db_prefix)keeprules\".\"store_type\" IS NOT NULL AND \"\($db_prefix)keeprules\".\"store_type\" != '\''plain'\'' AND \"entry_id\" NOT IN ( SELECT \"entry_id\" FROM \"\($db_prefix)filter_queue\" UNION SELECT \"entry_id\" FROM \"\($db_prefix)remove_queue\" )" ] else [] end
	) + [
		"DELETE FROM \"\($db_prefix)add_queue\" WHERE \"entry_id\" NOT IN (SELECT \"id\" FROM \"\($db_prefix)entries\")",
		"DELETE FROM \"\($db_prefix)remove_queue\" WHERE \"entry_id\" NOT IN (SELECT \"id\" FROM \"\($db_prefix)entries\")",
		"DELETE FROM \"\($db_prefix)filter_queue\" WHERE \"entry_id\" NOT IN (SELECT \"id\" FROM \"\($db_prefix)entries\")",
		"COMMIT TRANSACTION"
	] )|join(";")'
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" --argjson rules "${rules:?}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_UPDATE:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	rel_lock "${lock_code:?}"
}

process_add_queue_item() {
	local entry_id=$1
	[ "$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT COUNT(*) FROM \"${SNAPSHOTCTL_DB_PREFIX}add_queue\" WHERE \"entry_id\"=${entry_id:?}")" -gt 0 ] || { echo "snapshotctl: Not exist entry ${entry_id} from add queue" >&2; return 1; }

	local lock_code;	lock_code=$(cat <(echo "ADD:${entry_id:?}:") <(head --bytes=8 -q /dev/urandom) | sha256sum -b - | awk '{ print $1 }') || return
	acq_lock "${lock_code:?}" || return
	local entry;	entry=$(do_lock "${lock_code:?}" sqlite3 -json -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"${SNAPSHOTCTL_DB_PREFIX}entries\".* FROM \"${SNAPSHOTCTL_DB_PREFIX}add_queue\" LEFT JOIN \"${SNAPSHOTCTL_DB_PREFIX}entries\" ON \"${SNAPSHOTCTL_DB_PREFIX}add_queue\".\"entry_id\"=\"${SNAPSHOTCTL_DB_PREFIX}entries\".\"id\"" | jq -c '.[0]') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	local latest_entry;	latest_entry=$(do_lock "${lock_code:?}" sqlite3 -json -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT * FROM \"${SNAPSHOTCTL_DB_PREFIX}keep-entry-latests\"" | jq -c 'map({ key: .rule, value: del(.rule) })|from_entries') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	[ -n "${latest_entry}" ] || latest_entry='{}'
	local rules;	rules=$(do_lock "${lock_code:?}" sqlite3 -json -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT * FROM \"${SNAPSHOTCTL_DB_PREFIX}keeprules\"" | jq -c --argjson latest "${latest_entry:?}" 'map(.name as $rule_name|. + { latest: ($latest|.[$rule_name]) })') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	# shellcheck disable=SC2016
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_PROCESS_ADD_ITEM='( [
		"PRAGMA journal_mode = TRUNCATE",
		"BEGIN TRANSACTION"
	] +
	( $rules|map(
		if ((.bind_duration|type) == "null") or ((.latest|type) == "null") or (($entry|.date) >= (.latest.date + .bind_duration)) then "INSERT INTO \"\($db_prefix)keeplist\"( \"entry_id\", \"rule\" ) VALUES ( \($entry|.id), '\''\(.name)'\'' )" else empty end
	) ) + [
		"DELETE FROM \"\($db_prefix)add_queue\" WHERE \"entry_id\"=\($entry|.id)",
		"COMMIT TRANSACTION"
	] )|join(";")'
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" --argjson rules "${rules:?}" --argjson entry "${entry:?}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_PROCESS_ADD_ITEM:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	rel_lock "${lock_code:?}"
}

process_add_queue() {
	local add_queue;	add_queue=$(sqlite3 -json -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"entry_id\" FROM \"${SNAPSHOTCTL_DB_PREFIX}add_queue\" LEFT JOIN \"${SNAPSHOTCTL_DB_PREFIX}entries\" ON \"${SNAPSHOTCTL_DB_PREFIX}add_queue\".\"entry_id\"=\"${SNAPSHOTCTL_DB_PREFIX}entries\".\"id\" ORDER BY \"${SNAPSHOTCTL_DB_PREFIX}entries\".\"date\" ASC" | jq -c 'map(.entry_id)') || return
	[ -n "$add_queue" ] || add_queue='[]'
	for entry_id in $(jq -n -c --argjson add_queue "$add_queue" '$add_queue|.[]'); do
		process_add_queue_item "$entry_id" || return
	done
}

process_remove_queue_item() {
	local entry_id=$1
	[ "$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT COUNT(*) FROM \"${SNAPSHOTCTL_DB_PREFIX}remove_queue\" WHERE \"entry_id\"=${entry_id:?}")" -gt 0 ] || { echo "Snapshotctl: Not exist entry ${entry_id} from remove queue" >&2; return 1; }

	local db_location;	db_location=$(cd "$(dirname "${SNAPSHOTCTL_DB_PATH:?}")" && pwd) || return
	local backup_destination;	backup_destination=$(get_config | jq -r --arg db_location "${db_location:?}" '.backup_destination|if startswith("/") then . else ("\($db_location)/" + .) end') || return
	local item_path;	item_path=$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT ('${backup_destination:?}/' || \"fname\") FROM \"${SNAPSHOTCTL_DB_PREFIX}entries\" WHERE \"id\"=${entry_id:?}") || return
	local lock_code;	lock_code=$(cat <(echo "REMOVE:${entry_id:?}:") <(head --bytes=8 -q /dev/urandom) | sha256sum -b - | awk '{ print $1 }') || return
	acq_lock "${lock_code:?}" || return
	if [ -e "${item_path:?}" ]; then
		rm -f "${item_path:?}" || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	fi
	# shellcheck disable=SC2016
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_PROCESS_REMOVE_ITEM='( [
		"PRAGMA journal_mode = TRUNCATE",
		"DELETE FROM \"\($db_prefix)add_queue\" WHERE \"entry_id\"=\($entry_id)",
		"DELETE FROM \"\($db_prefix)filter_queue\" WHERE \"entry_id\"=\($entry_id)",
		"DELETE FROM \"\($db_prefix)remove_queue\" WHERE \"entry_id\"=\($entry_id)",
		"DELETE FROM \"\($db_prefix)entries\" WHERE \"id\"=\($entry_id)"
	] )|join(";")'
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" --arg entry_id "${entry_id:?}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_PROCESS_REMOVE_ITEM:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	rel_lock "${lock_code:?}"
}

process_remove_queue() {
	local remove_queue;	remove_queue=$(sqlite3 -json -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT DISTINCT \"entry_id\" FROM \"${SNAPSHOTCTL_DB_PREFIX}remove_queue\"" | jq -c 'map(.entry_id)') || return
	[ -n "$remove_queue" ] || remove_queue='[]'
	for item in $(jq -n -r --argjson remove_queue "$remove_queue" '$remove_queue|.[]'); do
		process_remove_queue_item "$item" || return
	done
}

filter_encode() {
	local lock_code=$1
	local entry_id=$2
	[ "$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT COUNT(*) FROM \"${SNAPSHOTCTL_DB_PREFIX}filter_queue\" WHERE \"entry_id\"=${entry_id:?}")" -gt 0 ] || { echo '{"return_code":1,"error":{"code":"ENTRY_NOT_EXIST","message":"Entry '"${entry_id}"' does not exist."}}'; return; }
	local fname=$3
	local filter=$4
	local db_location;	db_location=$(cd "$(dirname "${SNAPSHOTCTL_DB_PATH:?}")" && pwd) || { echo '{"error":{"code":"CANNOT_LOCATE_DATABASE_LOCATION","message":"Couldn'\''t locate DB location."}}'; return; }
	local backup_destination;	backup_destination=$(get_config | jq -r --arg db_location "${db_location:?}" '.backup_destination|if startswith("/") then . else ("\($db_location)/" + .) end') || { echo '{"error":{"code":"CANNOT_LOCATE_SNAPSHOT_DIRECTORY","message":"Couldn'\''t locate snapshot directory."}}'; return; }
	local worktmp;	worktmp=$(get_config | jq -r --arg db_location "${db_location:?}" '.worktmp|if startswith("/") then . else ("\($db_location)/" + .) end') || { echo '{"error":{"code":"CANNOT_LOCATE_WORKTMP","message":"Couldn'\''t locate working temp directory."}}'; return; }
	case $filter in
	plain)	echo '{"error":null,"new_fname":"'"${fname:?}"'"}'; return;;
	gzip)
		do_lock "${lock_code:?}" gzip --suffix=.gz "${worktmp:?}/${fname:?}" || return
		echo '{"error":null,"new_fname":"'"${fname}"'.gz"}'
		return;;
	zstd)
		do_lock "${lock_code:?}" zstd --rm "${worktmp:?}/${fname:?}" -o "${worktmp:?}/${fname:?}.zst" || return
		echo '{"error":null,"new_fname":"'"${fname}"'.zst"}'
		return;;
	xz)
		do_lock "${lock_code:?}" xz "${worktmp:?}/${fname:?}" || return
		echo '{"error":null,"new_fname":"'"${fname}"'.xz"}'
		return;;
	*)	echo '{"error":{"code":"INVALID_FILTER","message":"Specified invalid filter"}}'; return;;
	esac
}

filter_decode() {
	local lock_code=$1
	local entry_id=$2
	[ "$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT COUNT(*) FROM \"${SNAPSHOTCTL_DB_PREFIX}filter_queue\" WHERE \"entry_id\"=${entry_id:?}")" -gt 0 ] || { echo '{"return_code":1,"error":{"code":"ENTRY_NOT_EXIST","message":"Entry '"${entry_id}"' does not exist"}}'; return 1; }
	local fname=$3
	local filter=$4
	local db_location;	db_location=$(cd "$(dirname "${SNAPSHOTCTL_DB_PATH:?}")" && pwd) || { echo '{"error":{"code":"CANNOT_LOCATE_DATABASE_LOCATION","message":"Couldn'\''t locate DB location"}}'; return 1; }
	local backup_destination;	backup_destination=$(get_config | jq -r --arg db_location "${db_location:?}" '.backup_destination|if startswith("/") then . else ("\($db_location)/" + .) end') || { echo '{"error":{"code":"CANNOT_LOCATE_SNAPSHOT_DIRECTORY","message":"Couldn'\''t locate snapshot directory."}}'; return 1; }
	local worktmp;	worktmp=$(get_config | jq -r --arg db_location "${db_location:?}" '.worktmp|if startswith("/") then . else ("\($db_location)/" + .) end') || { echo '{"error":{"code":"CANNOT_LOCATE_WORKTMP","message":"Couldn'\''t locate working temp directory."}}'; return 1; }
	case $filter in
	plain)	echo '{"error":null,"new_fname":"'"${fname:?}"'"}'; return;;
	gzip)
		do_lock "${lock_code:?}" gunzip --suffix=.gz "${worktmp:?}/${fname:?}" || return
		echo '{"error":null,"new_fname":"'"${fname%.gz}"'"}'
		return;;
	zstd)
		do_lock "${lock_code:?}" zstd --decompress --rm "${worktmp:?}/${fname:?}" -o "${worktmp:?}/${fname%.zst}" || return
		echo '{"error":null,"new_fname":"'"${fname%.zst}"'"}'
		return;;
	xz)
		do_lock "${lock_code:?}" xz --decompress "${worktmp:?}/${fname:?}" || return
		echo '{"error":null,"new_fname":"'"${fname%.xz}"'"}'
		return;;
	*)	echo '{"error":{"code":"INVALID_FILTER","message":"Specified invalid filter"}}'; return 1;;
	esac
}

process_filter_queue_item() {
	local entry_id=$1
	[ "$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT COUNT(*) FROM \"${SNAPSHOTCTL_DB_PREFIX}filter_queue\" WHERE \"entry_id\"=${entry_id:?}")" -gt 0 ] || { echo "snapshotctl: Not exist entry ${entry_id} from compression queue" >&2; return 1; }
	local db_location;	db_location=$(cd "$(dirname "${SNAPSHOTCTL_DB_PATH:?}")" && pwd) || return
	local backup_destination;	backup_destination=$(get_config | jq -r --arg db_location "${db_location:?}" '.backup_destination|if startswith("/") then . else ("\($db_location)/" + .) end') || return
	local worktmp;	worktmp=$(get_config | jq -r --arg db_location "${db_location:?}" '.worktmp|if startswith("/") then . else ("\($db_location)/" + .) end') || return
	local convert_type;	convert_type=$(sqlite3 -json -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"${SNAPSHOTCTL_DB_PREFIX}keeprules\".* FROM \"${SNAPSHOTCTL_DB_PREFIX}keeplist\" LEFT JOIN \"${SNAPSHOTCTL_DB_PREFIX}keeprules\" ON \"${SNAPSHOTCTL_DB_PREFIX}keeplist\".\"rule\" = \"${SNAPSHOTCTL_DB_PREFIX}keeprules\".\"name\" WHERE \"entry_id\" = ${entry_id:?}" | jq -r 'def is_plain: (type == "null") or (type == "string" and . == "plain"); def is_compress: (type == "string") and ( . as $value|[ "gzip", "zstd", "xz" ]|map(. == $value)|any ); def is_difference: (type == "string") and (split("|")|.[0] == "rdiff") and (split("|")|map(is_plain or is_compress or . == "rdiff")|all); def is_valid: is_plain or is_compress or is_difference; if (map(.store_type|is_valid|not)|any) then ("snapshotctl: Invalid store type found. Please check config.\n"|halt_error) elif (map(.store_type|is_plain)|any) then "plain" elif (map(.store_type|is_compress)|any) then (map(select(.store_type|is_compress))|sort_by(.bind_duration)|reverse|.[0].store_type) else (sort_by(.bind_duration)|reverse|.[0].store_type) end') || return
	local fname;	fname=$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"fname\" FROM \"${SNAPSHOTCTL_DB_PREFIX}entries\" WHERE \"id\"=${entry_id:?}") || return
	local item_type;	item_type=$(sqlite3 -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT \"type\" FROM \"${SNAPSHOTCTL_DB_PREFIX}entries\" WHERE \"id\"=${entry_id:?}") || return
	if [ "${item_type:?}" == "${convert_type:?}" ]; then
		sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "PRAGMA journal_mode = TRUNCATE;DELETE FROM \"${SNAPSHOTCTL_DB_PREFIX}filter_queue\" WHERE \"entry_id\"=${entry_id:?}" >/dev/null
		return
	fi
	local lock_code;	lock_code=$(cat <(echo "COMPRESS:${entry_id:?}:") <(head --bytes=8 -q /dev/urandom) | sha256sum -b - | awk '{ print $1 }') || return
	acq_lock "${lock_code:?}" || return
	if [ -e "${worktmp:?}" ]; then
		do_lock "${lock_code:?}" rm -rf "${worktmp:?}/*" || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	else
		do_lock "${lock_code:?}" mkdir -p "${worktmp:?}" || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	fi
	do_lock "${lock_code:?}" cp -f -t "${worktmp:?}" "${backup_destination:?}/${fname:?}" || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	local new_fname="${fname:?}"
	for filter in $(echo "${item_type:?}" | jq -Rr 'split("|")|reverse|.[]')
	do
		local result;	result=$(filter_decode "${lock_code:?}" "${entry_id:?}" "${new_fname:?}" "${filter:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
		echo -E "${result:?}" | jq -r 'if (.error|type) != "null" then ("snapshotctl: Filter returned with error \(.error.code)\n\(.error.message)"|halt_error) else empty end' || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
		new_fname=$(echo -E "${result:?}" | jq -r --arg fname "${new_fname:?}" '.new_fname // $fname')
	done
	for filter in $(echo "${convert_type:?}" | jq -Rr 'split("|")|.[]')
	do
		local result;	result=$(filter_encode "${lock_code:?}" "${entry_id:?}" "${new_fname:?}" "${filter:?}") || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
		echo -E "${result:?}" | jq -r 'if (.error|type) != "null" then ("snapshotctl: Filter returned with error \(.error.code)\n\(.error.message)"|halt_error) else empty end' || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
		new_fname=$(echo -E "${result:?}" | jq -r --arg fname "${new_fname:?}" '.new_fname // $fname')
	done
	local size;	size=$(do_lock "${lock_code:?}" wc -c "${worktmp:?}/${new_fname:?}" | awk '{ print $1 }') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	local checksum;	checksum=$(do_lock "${lock_code:?}" sha256sum -b "${worktmp:?}/${new_fname:?}" | awk '{ print $1 }') || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	do_lock "${lock_code:?}" mv --no-clobber --target-directory="${backup_destination:?}/" "${worktmp:?}/${new_fname:?}" || { local rcode=$?; rel_lock "${lock_code:?}"; return $rcode; }
	# shellcheck disable=SC2016
	local -r SNAPSHOTCTL_JQ_DBSTATEMENT_PROCESS_FILTER='( [
		"PRAGMA journal_mode = TRUNCATE",
		"BEGIN TRANSACTION",
		"UPDATE \"\($db_prefix)entries\" SET \"fname\" = '\''\($new_fname)'\'', \"size\" = \($size), \"sha256\" = '\''\($sha256)'\'', \"type\" = '\''\($new_type)'\'' WHERE \"id\" = \($entry_id)",
		"DELETE FROM \"\($db_prefix)filter_queue\" WHERE \"entry_id\" = \($entry_id)",
		"COMMIT TRANSACTION"
	] )|join(";")'
	local sql_statement;	sql_statement=$(jq -n -r --arg db_prefix "${SNAPSHOTCTL_DB_PREFIX}" --argjson entry_id "${entry_id:?}" --arg new_fname "${new_fname:?}" --argjson size "${size:?}" --arg sha256 "${checksum:?}" --arg new_type "${convert_type:?}" "${SNAPSHOTCTL_JQ_DBSTATEMENT_PROCESS_FILTER:?}") || { local rcode=$?; rm -f "${backup_destination:?}/${new_fname:?}"; rel_lock "${lock_code:?}"; return $rcode; }
	sqlite3 "${SNAPSHOTCTL_DB_PATH:?}" "${sql_statement:?}" >/dev/null || { local rcode=$?; rm -f "${backup_destination:?}/${new_fname:?}"; rel_lock "${lock_code:?}"; return $rcode; }
	rm -f "${backup_destination:?}/${fname:?}"
	rel_lock "${lock_code:?}"
}

process_filter_queue() {
	local filter_queue;	filter_queue=$(sqlite3 -json -readonly "${SNAPSHOTCTL_DB_PATH:?}" "SELECT DISTINCT \"entry_id\" FROM \"${SNAPSHOTCTL_DB_PREFIX}filter_queue\"" | jq -c 'map(.entry_id)') || return
	[ -n "$filter_queue" ] || filter_queue='[]'
	for item in $(jq -n -r --argjson filter_queue "$filter_queue" '$filter_queue|.[]'); do
		process_filter_queue_item "$item" || return
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
	process_filter_queue || return
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
