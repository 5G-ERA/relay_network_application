FROM but5gera/relay_network_application:0.1.0

RUN git clone https://github.com/Kapim/era-5g-client.git \
    && cd era-5g-client \
    && python3 -m pip install -r requirements.txt \
    && ./pants package :: \
    && cd dist \
    && python3 -m pip install era_5g_client-0.2.0-py3-none-any.whl

COPY docker/client/start.sh /root/start.sh

RUN chmod +x /root/start.sh
