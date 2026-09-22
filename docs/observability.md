# Observabilidade do mcp-session-share

O endpoint `/metrics` (formato Prometheus, sem autenticação) expõe as
métricas do servidor. Este documento descreve como raspar esse endpoint e
visualizar os dados — nenhum dos dois é código deste repositório, então
ficam documentados aqui como referência.

## Métricas expostas

`/metrics` expõe:

- `session_share_rooms_active`
- `session_share_messages_total{kind}`
- `session_share_poll_latency_seconds`
- `session_share_scope_denied_total`
- `session_share_policy_denied_total`

Nenhuma métrica carrega conteúdo de room (texto, `display_name`, `room_id`,
`participant_id`) — só contadores agregados do servidor.

## Scrape

- `k8s/deployment.yaml`: o pod ganhou as annotations
  `prometheus.io/scrape: "true"`, `prometheus.io/port: "8000"`,
  `prometheus.io/path: "/metrics"` — convenção padrão de descoberta via
  `kubernetes_sd_configs` (`role: pod`) com relabeling nessas 3 annotations.
  Se o seu Prometheus já usa esse padrão (comum em clusters k3s/k8s), a
  descoberta é automática assim que o job de scrape existir.

- Se o seu Prometheus NÃO usa descoberta automática por annotation, adicione
  um job estático equivalente:

  ```yaml
  - job_name: mcp-session-share
    metrics_path: /metrics
    static_configs:
      - targets: ["mcp-session-share.mcp.svc.cluster.local:8000"]
  ```

  (usa o Service ClusterIP interno do cluster — `mcp-session-share`,
  namespace `mcp`, porta 8000 — não o NodePort externo.)

## Dashboard

- `docs/grafana-dashboard-mcp-session-share.json`: dashboard pronto (5 KPIs +
  3 painéis de série temporal — rooms ativas, mensagens/s por tipo, latência
  de poll p50/p95/p99, `SCOPE_DENIED`/`POLICY_DENIED` por segundo). Importe
  em Grafana → Dashboards → New → Import → colar o JSON, e aponte-o para o
  seu datasource Prometheus.

Depois de configurar scrape e importar o dashboard, confirme que
`session_share_rooms_active` retorna dado real (o painel "Rooms ativas"
deixando de mostrar "No data").
