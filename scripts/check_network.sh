#!/usr/bin/env bash
set -u

for variable in http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY; do
  if [[ -n "${!variable:-}" ]]; then
    echo "$variable=set"
  fi
done

if grep -Eqs '(http|https|all)_proxy|proxy=' \
  "$HOME/.bashrc" "$HOME/.profile" "$HOME/.bash_profile" 2>/dev/null; then
  echo "shell_proxy_config=present"
else
  echo "shell_proxy_config=absent"
fi

if git config --global --get http.proxy >/dev/null 2>&1; then
  echo "git_proxy=set"
else
  echo "git_proxy=absent"
fi

for url in \
  https://www.baidu.com/ \
  https://qt.gtimg.cn/q=sh600519 \
  http://data.10jqka.com.cn/funds/ggzjl/ \
  https://pypi.org/simple/; do
  status="$(curl -LIs --max-time 10 -o /dev/null -w '%{http_code}' "$url" 2>/dev/null || true)"
  [[ "$status" != "000" ]] || status="unreachable"
  echo "$url status=$status"
done
