import datadog


class DataDogHandler:
    def __init__(
        self,
        api_key: str,
        metric_prefix: str,
    ):
        self.metric_prefix = metric_prefix

        datadog.initialize(
            api_key=api_key,
        )

    def handle_service(
        self,
        cluster: str,
        service: str,
        tags: str,
        cpu_reservation: float,
        memory_reservation: float,
        hourly_cost: float,
        hourly_waste: float,
    ):
        dd_tags = [
            f"cluster:{cluster}",
            f"service:{service}",
        ]
        dd_tags.extend([f"{t['key']}:{t['value']}" for t in tags])
        datadog.api.Metric.send(
            metrics=[
                {
                    "metric": self._metric_name_cpu_reservation(),
                    "points": cpu_reservation,
                    "tags": dd_tags,
                    "type": "gauge",
                },
                {
                    "metric": self._metric_name_memory_reservation(),
                    "points": memory_reservation,
                    "tags": dd_tags,
                    "type": "gauge",
                },
                {
                    "metric": self._metric_name_hourly_cost(),
                    "points": hourly_cost,
                    "tags": dd_tags,
                    "type": "rate",
                },
                {
                    "metric": self._metric_name_hourly_waste(),
                    "points": hourly_waste,
                    "tags": dd_tags,
                    "type": "rate",
                },
            ]
        )

    def _metric_name_cpu_reservation(self):
        return f"{self.metric_prefix}.cpu_reservation"

    def _metric_name_memory_reservation(self):
        return f"{self.metric_prefix}.memory_reservation"

    def _metric_name_hourly_cost(self):
        return f"{self.metric_prefix}.hourly_cost"

    def _metric_name_hourly_waste(self):
        return f"{self.metric_prefix}.hourly_waste"
