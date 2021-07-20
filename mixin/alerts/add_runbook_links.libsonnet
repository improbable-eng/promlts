local utils = import '../lib/utils.libsonnet';

local lower(x) =
  local cp(c) = std.codepoint(c);
  local lowerLetter(c) =
    if cp(c) >= 65 && cp(c) < 91
    then std.char(cp(c) + 32)
    else c;
  std.join('', std.map(lowerLetter, std.stringChars(x)));

{
  _config+:: {
    runbookURLPattern: 'https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-%s',
    runbookURLAnchorsToLowercase: true,
  },

  prometheusAlerts+::
    local addRunbookURL(rule) = rule {
      [if 'alert' in rule && !('runbook_url' in rule.annotations) then 'annotations']+: {
        runbook_url: if $._config.runbookURLAnchorsToLowercase then $._config.runbookURLPattern % lower(rule.alert) else $._config.runbookURLPattern % rule.alert,
      },
    };
    utils.mapRuleGroups(addRunbookURL),
}
