from typing import Type, Union

from etl_components.etl_process import EtlFilmWorkProcess, EtlGenreProcess, EtlPersonProcess

PipeLineType = Type[
    Union[
        EtlFilmWorkProcess,
        EtlPersonProcess,
        EtlGenreProcess
    ]
]
