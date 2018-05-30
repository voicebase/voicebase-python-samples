from CallbackTester import CallbackTester
import multiprocessing as mp

# Parallel pattern inspired by:
# https://stackoverflow.com/questions/43078980/python-multiprocessing-with-generator

class ParallelCallbackTester(CallbackTester):
    def __init__(self, **kwargs):
        super(ParallelCallbackTester, self).__init__(**kwargs)
        self.parallelism = int(kwargs.get('parallelism', 4))
        #self.max_queue_size = 5 * 1000 * 1000
        self.max_queue_size = 100
        self.request_queue = mp.Queue(maxsize = self.max_queue_size)
        self.response_queue = mp.Queue(maxsize = self.max_queue_size)


    @classmethod
    def _add_command_line_args(cls, parser):
        super(ParallelCallbackTester, cls)._add_command_line_args(parser)
        parser.add_argument(
            '--parallelism',
            help = 'Level of parallelism (# concurrent, default 4)',
            required = False
        )

    def test(self, results):
        def parallel_processor(parallel_callback_tester):
            print('[Parallel] Initialized parallel processor')
            while True:
                download = parallel_callback_tester.request_queue.get()
                if download is None:
                    break

                test = parallel_callback_tester.test_one(download)
                self.response_queue.put(test)


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
            callback = self.response_queue.get()
            print('[Parallel] consumed', i)
            yield callback

        print('[Parallel] done consuming')

        pool.close()
        pool.join()

    @classmethod
    def _initialize_tester(cls, args):
        print('parallelism: ', args.parallelism)
        return ParallelCallbackTester(
            input_csv = args.inputCsv,
            download_directory = args.downloadDirectory,
            output_csv = args.outputCsv,
            destination_url = args.destinationUrl,
            parallelism = args.parallelism
        )

if __name__ == '__main__':
    ParallelCallbackTester.main()
