#!/usr/bin/env bash
set -euo pipefail

fail() {
    printf 'verify-dnat: %s\n' "$1" >&2
    exit 1
}

check_table() {
    local family=$1
    local save_command=$2
    local rules dnat_rules rule rule_index=0 bad_rules=0

    rules=$($save_command -t nat)
    dnat_rules=$(printf '%s\n' "$rules" \
        | awk '$1 == "-A" && $2 == "PREROUTING" && $0 ~ /(^| )-j DNAT( |$)/')

    if [[ -z "$dnat_rules" ]]; then
        printf 'verify-dnat: %s has no PREROUTING DNAT rules\n' "$family"
        return 0
    fi

    while IFS= read -r rule; do
        [[ -n "$rule" ]] || continue
        rule_index=$((rule_index + 1))
        if ! printf '%s\n' "$rule" | grep -Eq '(^| )-i [^ ]+'; then
            printf 'verify-dnat: %s PREROUTING DNAT rule %s has no ingress interface match\n' \
                "$family" "$rule_index" >&2
            bad_rules=$((bad_rules + 1))
        fi
    done <<< "$dnat_rules"

    (( bad_rules == 0 )) \
        || fail "$family has $bad_rules PREROUTING DNAT rule(s) without an ingress interface"
    printf 'verify-dnat: %s has %s scoped PREROUTING DNAT rule(s)\n' "$family" "$rule_index"
}

command -v iptables-save >/dev/null 2>&1 || fail "iptables-save is required"
check_table IPv4 iptables-save

if command -v ip6tables-save >/dev/null 2>&1; then
    check_table IPv6 ip6tables-save
else
    [[ -r /proc/sys/net/ipv6/conf/all/forwarding ]] \
        || fail "IPv6 forwarding state is unreadable and ip6tables-save is unavailable"
    read -r ipv6_forwarding </proc/sys/net/ipv6/conf/all/forwarding
    if [[ "$ipv6_forwarding" == 1 ]]; then
        fail "IPv6 forwarding is enabled but ip6tables-save is unavailable"
    fi
    printf 'verify-dnat: IPv6 forwarding is disabled; ip6tables-save unavailable\n'
fi
