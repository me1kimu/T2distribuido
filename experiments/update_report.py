import json
import re

def load_json(path):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception:
        return {}

sync_base = load_json('results/sync_base.json')
kafka_1 = load_json('results/kafka_1_consumer.json')
kafka_3 = load_json('results/kafka_3_consumers.json')
kafka_fail = load_json('results/kafka_temporal_failure.json')
kafka_spike = load_json('results/kafka_spike.json')

tex_file = 'informe/lab01_informe.tex'
with open(tex_file, 'r') as f:
    content = f.read()

# Replace the "Resultados" section or add it if it doesn't exist
results_section = f"""\\section{{Resultados de Evaluaci\\'on Experimental}}

Se ejecutaron los siguientes 5 escenarios experimentales, inyectando tr\\'afico mediante la herramienta de load testing local:

\\subsection{{Sistema Base S\\'incrono}}
\\begin{{itemize}}
    \\item \\textbf{{Throughput}}: {sync_base.get('throughput_qps', 0):.2f} req/s
    \\item \\textbf{{Latencia p50}}: {sync_base.get('latency_ms_p50', 0):.2f} ms
    \\item \\textbf{{Latencia p95}}: {sync_base.get('latency_ms_p95', 0):.2f} ms
    \\item \\textbf{{P\\'erdida}}: No aplicable (s\\'incrono).
\\end{{itemize}}

\\subsection{{Kafka + 1 Consumidor}}
\\begin{{itemize}}
    \\item \\textbf{{Throughput}}: {kafka_1.get('throughput_qps', 0):.2f} req/s
    \\item \\textbf{{Latencia p50}}: {kafka_1.get('latency_ms_p50', 0):.2f} ms
    \\item \\textbf{{Latencia p95}}: {kafka_1.get('latency_ms_p95', 0):.2f} ms
    \\item \\textbf{{Hit rate}}: {kafka_1.get('hit_rate', 0)*100:.1f}\\%
\\end{{itemize}}

\\subsection{{Kafka + 3 Consumidores}}
\\begin{{itemize}}
    \\item \\textbf{{Throughput}}: {kafka_3.get('throughput_qps', 0):.2f} req/s
    \\item \\textbf{{Latencia p50}}: {kafka_3.get('latency_ms_p50', 0):.2f} ms
    \\item \\textbf{{Latencia p95}}: {kafka_3.get('latency_ms_p95', 0):.2f} ms
    \\item \\textbf{{Hit rate}}: {kafka_3.get('hit_rate', 0)*100:.1f}\\%
\\end{{itemize}}

\\subsection{{Falla Temporal y Recuperaci\\'on}}
Durante este escenario, el \texttt{{response-service}} se detuvo artificialmente durante 15 segundos mientras las consultas segu\\'ian ingresando:
\\begin{{itemize}}
    \\item \\textbf{{Recovery Rate}}: {kafka_fail.get('recovery_rate', 0)*100:.1f}\\%
    \\item \\textbf{{Reintentos emitidos}}: {kafka_fail.get('retries', 0)}
    \\item \\textbf{{Consultas a DLQ}}: {kafka_fail.get('dlqs', 0)}
    \\item \\textbf{{Latencia prom. recuperaci\\'on}}: {kafka_fail.get('recovery_latency_ms_avg', 0):.2f} ms
\\end{{itemize}}

\\subsection{{Spike de Tr\\'afico}}
Se enviaron 3000 consultas instant\\'aneamente a Kafka para observar el backlog:
\\begin{{itemize}}
    \\item \\textbf{{Throughput sostenido}}: {kafka_spike.get('throughput_qps', 0):.2f} req/s
    \\item \\textbf{{Latencia p95}}: {kafka_spike.get('latency_ms_p95', 0):.2f} ms
\\end{{itemize}}
"""

if '\\section{Resultados de Evaluaci\\' in content:
    # Use regex to replace the section and everything up to the next \section or end of document
    content = re.sub(r'\\section\{Resultados de Evaluaci\\\'on Experimental\}.*?(?=\\section|\Z)', results_section + '\n', content, flags=re.DOTALL)
else:
    # Append before the Discussion or Conclusion
    content = content.replace('\\section{Discusi\\\'on}', results_section + '\n\\section{Discusi\\\'on}')

with open(tex_file, 'w') as f:
    f.write(content)
