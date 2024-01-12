python_sources(name="lib")

resources(name="py.typed", sources=["py.typed"])

pex_binary(
    name="client",
    entry_point="client.py:main",
    inherit_path="prefer",
    dependencies=["era_5g_relay_network_application/compatibility:compatibility"],
)
pex_binary(
    name="server",
    entry_point="server.py:main",
    inherit_path="prefer",
    dependencies=["era_5g_relay_network_application/compatibility:compatibility"],
)

python_distribution(
    name="dist",
    dependencies=[
        ":lib",
        ":py.typed",
        "era_5g_relay_network_application/data",
        "era_5g_relay_network_application/compatibility:compatibility",
    ],
    wheel=True,
    sdist=True,
    provides=setup_py(
        name="era_5g_relay_network_application",
        version="0.1.0",
        description="Relay Network Application",
        author="Michal Kapinus",
        author_email="ikapinus@fit.vutbr.cz",
        license="LGPL",
        python_requires=">=3.8",
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
            "Operating System :: OS Independent",
            "Intended Audience :: Developers",
        ],
    ),
)
