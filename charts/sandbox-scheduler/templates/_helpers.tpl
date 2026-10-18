{{- define "sandbox-scheduler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sandbox-scheduler.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "sandbox-scheduler.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "sandbox-scheduler.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "sandbox-scheduler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sandbox-scheduler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "sandbox-scheduler.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "sandbox-scheduler.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "sandbox-scheduler.terminationGracePeriodSeconds" -}}
{{- $configured := int (default 30 .Values.scheduler.terminationGracePeriodSeconds) -}}
{{- $raw := toString (default "15s" .Values.scheduler.claimTimeout) -}}
{{- $hours := 0 -}}
{{- if regexMatch "[0-9]+h" $raw -}}
{{- $hours = int (trimSuffix "h" (regexFind "[0-9]+h" $raw)) -}}
{{- end -}}
{{- $mins := 0 -}}
{{- if regexMatch "[0-9]+m($|[^s])" $raw -}}
{{- $mins = int (regexFind "[0-9]+" (regexFind "[0-9]+m($|[^s])" $raw)) -}}
{{- end -}}
{{- $secs := 0 -}}
{{- if regexMatch "(^|[^m])[0-9]+s" $raw -}}
{{- $secs = int (regexFind "[0-9]+" (regexFind "(^|[^m])[0-9]+s" $raw)) -}}
{{- end -}}
{{- $msExtra := 0 -}}
{{- if regexMatch "[0-9]+ms" $raw -}}
{{- $ms := int (trimSuffix "ms" (regexFind "[0-9]+ms" $raw)) -}}
{{- $msExtra = div (add $ms 999) 1000 -}}
{{- end -}}
{{- $claimTimeoutSecs := add (add (mul $hours 3600) (mul $mins 60)) (add $secs $msExtra) -}}
{{- $minGrace := add $claimTimeoutSecs 10 -}}
{{- if gt $minGrace $configured -}}
{{- $minGrace -}}
{{- else -}}
{{- $configured -}}
{{- end -}}
{{- end -}}

