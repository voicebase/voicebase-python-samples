from Downloader import Downloader
import multiprocessing as mp

# Parallel pattern inspired by:
# https://stackoverflow.com/questions/43078980/python-multiprocessing-with-generator

class ParallelDownloader(Downloader):
    def __init__(self, **kwargs):
        super(ParallelDownloader, self).__init__(**kwargs)
        self.parallelism = int(kwargs.get('parallelism', 4))
        #self.max_queue_size = 5 * 1000 * 1000
        self.max_queue_size = 100
        self.request_queue = mp.Queue(maxsize = self.max_queue_size)
        self.response_queue = mp.Queue(maxsize = self.max_queue_size)


    @classmethod
    def _add_command_line_args(cls, parser):
        super(ParallelDownloader, cls)._add_command_line_args(parser)
        parser.add_argument(
            '--parallelism',
            help = 'Level of parallelism (# concurrent, default 4)',
            required = False
        )

    def download(self, results):
        def parallel_processor(parallel_downloader):
            print('[Parallel] Initialized parallel processor')
            while True:
                result = parallel_downloader.request_queue.get()
                if result is None:
                    break

                download = parallel_downloader.download_one(result)
                self.response_queue.put(download)

        pool = mp.Pool(
            self.parallelism,
            initializer = parallel_processor,
            initargs = [self]
        )

        total_results = 0
        for result in results:
            # This request will block when queue exceeds maximum size
            self.request_queue.put(result)
            total_results = total_results + 1

        # Put a None for each worker to signal stop
        for _ in range(self.parallelism):
            self.request_queue.put(None)

        for i in range(total_results):
            download = self.response_queue.get()
            print('[Parallel] consumed', i)
            yield download

        print('[Parallel] done consuming')

        pool.close()
        pool.join()

    def _initialize_downloader(cls, args):
        print('parallelism: ', args.parallelism)
        return ParallelDownloader(
            input_csv = args.inputCsv,
            download_directory = args.downloadDirectory,
            output_csv = args.outputCsv,
            token = args.token,
            parallelism = args.parallelism
        )

if __name__ == '__main__':
    ParallelDownloader.main()
