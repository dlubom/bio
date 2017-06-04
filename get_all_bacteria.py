from Bio.Blast import NCBIWWW
from pathlib2 import Path

data_in_catalog = "DataIn"
data_out_catalog = 'DataOut2'

data_in = Path('.') / data_in_catalog
bacteria_paths = [x for x in data_in.iterdir() if x.is_file()]

for microbe_path in bacteria_paths:
    print("Processing: {0}".format(microbe_path))

    with microbe_path.open() as f:
        rna = f.read()

        if rna == "":
            print "EMPTY FILE !!!"
            continue

        microbe = microbe_path.stem

        p = Path('.')
        path = p / data_out_catalog / microbe

        path.mkdir(parents=True, exist_ok=True)

        result_handle = NCBIWWW.qblast(megablast=True, program="blastn", database="nt", sequence=rna, format_type="XML")
        with open("{0}/{1}.xml".format(str(path), microbe), "w") as out_handle:
            out_handle.write(result_handle.read())
            result_handle.close()
        print("\txml done.")

        result_handle = NCBIWWW.qblast(megablast=True, program="blastn", database="nt", sequence=rna,
                                       format_type="HTML")
        with open("{0}/{1}.html".format(str(path), microbe), "w") as out_handle:
            out_handle.write(result_handle.read())
            result_handle.close()
        print("\thtml done.")

print("Ended.")
